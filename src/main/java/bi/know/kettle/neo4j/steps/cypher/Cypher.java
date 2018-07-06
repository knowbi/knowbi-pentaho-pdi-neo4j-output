package bi.know.kettle.neo4j.steps.cypher;


import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cypher extends BaseStep implements StepInterface {

  public Cypher( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                 TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    CypherMeta meta = (CypherMeta) smi;
    CypherData data = (CypherData)sdi;

    // Is the step getting input?
    //
    List<StepMeta> steps = getTransMeta().findPreviousSteps( getStepMeta() );
    data.hasInput = steps!=null && steps.size()>0;

    // Connect to Neo4j using info in Neo4j JDBC connection metadata...
    //
    try {
      data.neoConnection = NeoConnectionUtils.getConnectionFactory( metaStore ).loadElement( meta.getConnectionName() );
    } catch ( MetaStoreException e ) {
      log.logError("Could not load Neo4j connection '"+meta.getConnectionName()+"' from the metastore", e);
      return false;
    }

    data.batchSize = Const.toLong(environmentSubstitute( meta.getBatchSize()), 1);

    try {
      data.driver = data.neoConnection.getDriver(log);
    } catch(Exception e) {
      log.logError( "Unable to get or create Neo4j database driver for database '"+data.neoConnection.getName()+"'", e);
      return false;
    }

    return super.init( smi, sdi );
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    CypherData data = (CypherData)sdi;

    if (data.outputCount >0) {
      data.transaction.success();
      data.transaction.close();
    }
    if (data.session!=null) {
      data.session.close();
    }
    data.driver.close();

    super.dispose( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    CypherMeta meta = (CypherMeta) smi;
    CypherData data = (CypherData)sdi;

    // Input row
    //
    Object[] row = new Object[0];

    // Only if we actually have previous steps to read from...
    // This way the step also acts as an GraphOutput query step
    //
    if (data.hasInput) {
      // Get a row of data from previous steps...
      //
      row = getRow();
      if ( row == null ) {
        // Signal next step(s) we're done processing
        //
        setOutputDone();
        return false;
      }
    }

    if (first) {
      first=false;

      // get the output fields...
      //
      data.outputRowMeta = data.hasInput ? getInputRowMeta().clone() : new RowMeta();
      meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, metaStore );

      // Create a session
      //
      data.session = data.driver.session();

      // Get parameter field indexes
      data.fieldIndexes = new int[meta.getParameterMappings().size()];
      for (int i=0;i<meta.getParameterMappings().size();i++) {
        String field = meta.getParameterMappings().get(i).getField();
        data.fieldIndexes[i] = getInputRowMeta().indexOfValue( field );
        if (data.fieldIndexes[i]<0) {
          throw new KettleStepException( "Unable to find parameter field '"+field );
        }
      }

      data.cypherFieldIndex = -1;
      if (data.hasInput) {
        data.cypherFieldIndex = getInputRowMeta().indexOfValue( meta.getCypherField() );
        if ( meta.isCypherFromField() && data.cypherFieldIndex < 0 ) {
          throw new KettleStepException( "Unable to find cypher field '" + meta.getCypherField() + "'" );
        }
      }
      data.cypher = environmentSubstitute( meta.getCypher() );
    }

    if (meta.isCypherFromField()) {
      data.cypher = getInputRowMeta().getString(row, data.cypherFieldIndex);
    }

    // Do the value mapping and conversion to the parameters
    //
    Map<String, Object> parameters = new HashMap<>();
    for (int i=0;i<meta.getParameterMappings().size();i++) {
      ParameterMapping mapping = meta.getParameterMappings().get( i );
      ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta( data.fieldIndexes[i] );
      Object valueData = row[data.fieldIndexes[i]];
      GraphPropertyType propertyType = GraphPropertyType.parseCode( mapping.getNeoType() );
      if (propertyType==null) {
        throw new KettleException( "Unable to convert to unknown property type for field '"+valueMeta.toStringMeta()+"'" );
      }
      Object neoValue = propertyType.convertFromKettle(valueMeta, valueData);
      parameters.put(mapping.getParameter(), neoValue);
    }

    // Execute the cypher with all the parameters...
    //
    StatementResult result;
    if (data.batchSize<=1) {
      result = data.session.run( data.cypher, parameters );
    } else {
      if (data.outputCount ==0) {
        data.transaction = data.session.beginTransaction();
      }
      result = data.transaction.run( data.cypher, parameters );
      data.outputCount++;
      incrementLinesOutput();

      if (data.outputCount >=data.batchSize) {
        data.transaction.success();
        data.transaction.close();
        data.outputCount =0;
      }
    }
    int rowsWritten = 0;

    while ( result.hasNext() ) {
      Record record = result.next();

      // Create output row
      Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );

      // add result values...
      //
      int index=data.hasInput ? getInputRowMeta().size() : 0;
      for ( ReturnValue returnValue : meta.getReturnValues() ) {
        Value recordValue = record.get( returnValue.getName() );
        ValueMetaInterface targetValueMeta = data.outputRowMeta.getValueMeta(index);
        Object value = null;
        if (recordValue!=null) {
          try {
            switch ( targetValueMeta.getType() ) {
              case ValueMetaInterface.TYPE_STRING:
                value = recordValue.toString();
                break;
              case ValueMetaInterface.TYPE_INTEGER:
                value = recordValue.asLong();
                break;
              case ValueMetaInterface.TYPE_NUMBER:
                value = recordValue.asDouble();
                break;
              case ValueMetaInterface.TYPE_BOOLEAN:
                value = recordValue.asBoolean();
                break;
              case ValueMetaInterface.TYPE_BIGNUMBER:
                value = new BigDecimal( recordValue.asString() );
                break;
              case ValueMetaInterface.TYPE_DATE:
                LocalDate localDate = recordValue.asLocalDate();
                value = java.sql.Date.valueOf( localDate );
                break;
              case ValueMetaInterface.TYPE_TIMESTAMP:
                LocalDateTime localDateTime = recordValue.asLocalDateTime();
                value = java.sql.Timestamp.valueOf( localDateTime );
                break;
              default:
                throw new KettleException( "Unable to convert Neo4j data to type " + targetValueMeta.toStringMeta() );
            }
          } catch(Exception e) {
            throw new KettleException("Unable to convert Neo4j record value '"+returnValue.getName()+"' to type : "+targetValueMeta.getTypeDesc(), e);
          }
        }
        outputRow[index++] = value;
      }

      // Pass the rows to the next steps
      //
      putRow( data.outputRowMeta, outputRow);
      rowsWritten++;
    }

    if (data.hasInput && rowsWritten==0) {
      // At least pass input row

      // Create output row
      Object[] outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );

      // Pass the rows to the next steps
      //
      putRow( data.outputRowMeta, outputRow);
    }

    // Only keep executing if we have input rows...
    //
    if (data.hasInput) {
      return true;
    } else {
      setOutputDone();
      return false;
    }
  }
}
