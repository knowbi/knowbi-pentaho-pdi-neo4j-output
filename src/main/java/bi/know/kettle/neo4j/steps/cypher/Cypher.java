package bi.know.kettle.neo4j.steps.cypher;


import bi.know.kettle.neo4j.shared.MetaStoreUtil;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.kettle.core.data.GraphData;
import org.neo4j.kettle.core.data.GraphPropertyDataType;
import org.neo4j.kettle.model.GraphPropertyType;
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
import org.pentaho.metastore.stores.xml.XmlMetaStore;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cypher extends BaseStep implements StepInterface {

  private CypherMeta meta;
  private CypherData data;

  public Cypher( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                 TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    meta = (CypherMeta) smi;
    data = (CypherData) sdi;

    // Is the step getting input?
    //
    List<StepMeta> steps = getTransMeta().findPreviousSteps( getStepMeta() );
    data.hasInput = steps != null && steps.size() > 0;

    // Connect to Neo4j
    //
    if ( StringUtils.isEmpty( meta.getConnectionName() ) ) {
      log.logError( "You need to specify a Neo4j connection to use in this step" );
      return false;
    }
    try {
      // To correct lazy programmers who built certain PDI steps...
      //
      data.metaStore = MetaStoreUtil.findMetaStore( this );
      data.neoConnection = NeoConnectionUtils.getConnectionFactory( data.metaStore ).loadElement( meta.getConnectionName() );
      if (data.neoConnection==null) {
        log.logError("Connection '"+meta.getConnectionName()+"' could not be found in the metastore "+MetaStoreUtil.getMetaStoreDescription(metaStore));
        return false;
      }
      data.neoConnection.initializeVariablesFrom( this );

    } catch ( MetaStoreException e ) {
      log.logError( "Could not gencsv Neo4j connection '" + meta.getConnectionName() + "' from the metastore", e );
      return false;
    }

    data.batchSize = Const.toLong( environmentSubstitute( meta.getBatchSize() ), 1 );

    try {
      createDriverSession();
    } catch ( Exception e ) {
      log.logError( "Unable to get or create Neo4j database driver for database '" + data.neoConnection.getName() + "'", e );
      return false;
    }

    return super.init( smi, sdi );
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    meta = (CypherMeta) smi;
    data = (CypherData) sdi;

    wrapUpTransaction();
    closeSessionDriver();

    super.dispose( smi, sdi );
  }

  private void closeSessionDriver() {
    if ( data.session != null ) {
      data.session.close();
    }
  }

  private void createDriverSession() {
    data.session = data.neoConnection.getSession(log);
  }

  private void reconnect() {
    closeSessionDriver();

    log.logBasic( "RECONNECTING to database" );

    // Wait for 30 seconds before reconnecting.
    // Let's give the server a breath of fresh air.
    try {
      Thread.sleep( 30000 );
    } catch ( InterruptedException e ) {
      // ignore sleep interrupted.
    }

    createDriverSession();
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (CypherMeta) smi;
    data = (CypherData) sdi;

    // Input row
    //
    Object[] row = new Object[ 0 ];

    // Only if we actually have previous steps to read from...
    // This way the step also acts as an GraphOutput query step
    //
    if ( data.hasInput ) {
      // Get a row of data from previous steps...
      //
      row = getRow();
      if ( row == null ) {

        // See if there's anything left to write...
        //
        wrapUpTransaction();

        // Signal next step(s) we're done processing
        //
        setOutputDone();
        return false;
      }
    }

    if ( first ) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = data.hasInput ? getInputRowMeta().clone() : new RowMeta();
      meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, data.metaStore );

      // Create a session
      //
      createDriverSession();

      // Get parameter field indexes
      data.fieldIndexes = new int[ meta.getParameterMappings().size() ];
      for ( int i = 0; i < meta.getParameterMappings().size(); i++ ) {
        String field = meta.getParameterMappings().get( i ).getField();
        data.fieldIndexes[ i ] = getInputRowMeta().indexOfValue( field );
        if ( data.fieldIndexes[ i ] < 0 ) {
          throw new KettleStepException( "Unable to find parameter field '" + field );
        }
      }

      data.cypherFieldIndex = -1;
      if ( data.hasInput ) {
        data.cypherFieldIndex = getInputRowMeta().indexOfValue( meta.getCypherField() );
        if ( meta.isCypherFromField() && data.cypherFieldIndex < 0 ) {
          throw new KettleStepException( "Unable to find cypher field '" + meta.getCypherField() + "'" );
        }
      }
      data.cypher = environmentSubstitute( meta.getCypher() );

      data.unwindList = new ArrayList<>();
      data.unwindMapName = environmentSubstitute( meta.getUnwindMapName() );

      data.cypherStatements = new ArrayList<>();
    }

    if ( meta.isCypherFromField() ) {
      data.cypher = getInputRowMeta().getString( row, data.cypherFieldIndex );
    }

    // Do the value mapping and conversion to the parameters
    //
    Map<String, Object> parameters = new HashMap<>();
    for ( int i = 0; i < meta.getParameterMappings().size(); i++ ) {
      ParameterMapping mapping = meta.getParameterMappings().get( i );
      ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta( data.fieldIndexes[ i ] );
      Object valueData = row[ data.fieldIndexes[ i ] ];
      GraphPropertyType propertyType = GraphPropertyType.parseCode( mapping.getNeoType() );
      if ( propertyType == null ) {
        throw new KettleException( "Unable to convert to unknown property type for field '" + valueMeta.toStringMeta() + "'" );
      }
      Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
      parameters.put( mapping.getParameter(), neoValue );
    }

    // Create a map between the return value and the source type so we can do the appropriate mapping later...
    //
    data.returnSourceTypeMap = new HashMap<>(  );
    for (ReturnValue returnValue : meta.getReturnValues()) {
      if (StringUtils.isNotEmpty( returnValue.getSourceType() )) {
        String name = returnValue.getName();
        GraphPropertyDataType type = GraphPropertyDataType.parseCode( returnValue.getSourceType() );
        data.returnSourceTypeMap.put(name, type);
      }
    }

    if ( meta.isUsingUnwind() ) {
      data.unwindList.add( parameters );
      data.outputCount++;

      if ( data.outputCount >= data.batchSize ) {
        writeUnwindList();
      }
    } else {

      // Execute the cypher with all the parameters...
      //
      try {
        runCypherStatement( row, data.cypher, parameters );
      } catch ( ServiceUnavailableException e ) {
        // retry once after reconnecting.
        // This can fix certain time-out issues
        //
        if (meta.isRetrying()) {
          reconnect();
          runCypherStatement( row, data.cypher, parameters );
        } else {
          throw e;
        }
      } catch(KettleException e) {
        setErrors( 1 );
        stopAll();
        throw e;
      }
    }

    // Only keep executing if we have input rows...
    //
    if ( data.hasInput ) {
      return true;
    } else {
      setOutputDone();
      return false;
    }
  }

  private void runCypherStatement( Object[] row, String cypher, Map<String, Object> parameters ) throws KettleException {
    data.cypherStatements.add( new CypherStatement( row, cypher, parameters ) );
    if ( data.cypherStatements.size() >= data.batchSize || !data.hasInput) {
      runCypherStatementsBatch();
    }
  }

  private void runCypherStatementsBatch() throws KettleException {

    if (data.cypherStatements==null || data.cypherStatements.size()==0) {
      // Nothing to see here, move along
      return;
    }

    // Execute all the statements in there in one transaction...
    //
    TransactionWork<Integer> transactionWork = transaction -> {

      for ( CypherStatement cypherStatement : data.cypherStatements ) {
        Result result = transaction.run( cypherStatement.getCypher(), cypherStatement.getParameters() );
        try {
          getResultRows( result, cypherStatement.getRow(), false );


        } catch(Exception e) {
          throw new RuntimeException( "Error parsing result of cypher statement '"+cypherStatement.getCypher()+"'", e );
        }
      }

      return data.cypherStatements.size();
    };

    try {
      int nrProcessed;
      if ( meta.isReadOnly() ) {
        nrProcessed = data.session.readTransaction( transactionWork );
        setLinesInput( getLinesInput() + data.cypherStatements.size() );
      } else {
        nrProcessed = data.session.writeTransaction( transactionWork );
        setLinesOutput( getLinesOutput() + data.cypherStatements.size() );
      }

      if (log.isDebug()) {
        logDebug( "Processed "+nrProcessed+" statements" );
      }

      // Clear out the batch of statements.
      //
      data.cypherStatements.clear();

    } catch ( Exception e ) {
      throw new KettleException( "Unable to execute batch of cypher statements ("+data.cypherStatements.size()+")", e );
    }
  }

  private List<Object[]> writeUnwindList() throws KettleException {
    HashMap<String, Object> unwindMap = new HashMap<>();
    unwindMap.put( data.unwindMapName, data.unwindList );
    List<Object[]> resultRows = null;
    CypherTransactionWork cypherTransactionWork = new CypherTransactionWork( this, new Object[ 0 ], true, data.cypher, unwindMap );
    try {
      try {
        if ( meta.isReadOnly() ) {
          data.session.readTransaction( cypherTransactionWork );
        } else {
          data.session.writeTransaction( cypherTransactionWork );
        }
      } catch ( ServiceUnavailableException e ) {
        // retry once after reconnecting.
        // This can fix certain time-out issues
        //
        if (meta.isRetrying()) {
          reconnect();
          if ( meta.isReadOnly() ) {
            data.session.readTransaction( cypherTransactionWork );
          } else {
            data.session.writeTransaction( cypherTransactionWork );
          }
        } else {
          throw e;
        }
      }
    } catch ( Exception e ) {
      data.session.close();
      stopAll();
      setErrors( 1L );
      setOutputDone();
      throw new KettleException( "Unexpected error writing unwind list to Neo4j", e );
    }
    setLinesOutput( getLinesOutput() + data.unwindList.size() );
    data.unwindList.clear();
    data.outputCount = 0;
    return resultRows;
  }

  public void getResultRows( Result result, Object[] row, boolean unwind ) throws KettleException {

    if ( result != null ) {

      if ( meta.isReturningGraph() ) {

        GraphData graphData = new GraphData( result );
        graphData.setSourceTransformationName( getTransMeta().getName() );
        graphData.setSourceStepName( getStepname() );

        // Create output row
        Object[] outputRowData;
        if ( unwind ) {
          outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
        } else {
          outputRowData = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
        }
        int index = data.hasInput && !unwind ? getInputRowMeta().size() : 0;

        outputRowData[ index ] = graphData;
        putRow( data.outputRowMeta, outputRowData );

      } else {

        while ( result.hasNext() ) {
          Record record = result.next();

          // Create output row
          Object[] outputRow;
          if ( unwind ) {
            outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          } else {
            outputRow = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
          }

          // add result values...
          //
          int index = data.hasInput && !unwind ? getInputRowMeta().size() : 0;
          for ( ReturnValue returnValue : meta.getReturnValues() ) {
            Value recordValue = record.get( returnValue.getName() );
            ValueMetaInterface targetValueMeta = data.outputRowMeta.getValueMeta( index );
            Object value = null;
            GraphPropertyDataType neoType = data.returnSourceTypeMap.get( returnValue.getName() );
            if ( recordValue != null && !recordValue.isNull()) {
              try {
                switch ( targetValueMeta.getType() ) {
                  case ValueMetaInterface.TYPE_STRING:
                    value = convertToString(recordValue, neoType);
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
                    if (neoType!=null) {
                     // Standard...
                     switch(neoType) {
                       case LocalDateTime: {
                         LocalDateTime localDateTime = recordValue.asLocalDateTime();
                         value = java.sql.Date.valueOf( localDateTime.toLocalDate() );
                         break;
                       }
                       case Date: {
                         LocalDate localDate = recordValue.asLocalDate();
                         value = java.sql.Date.valueOf( localDate );
                         break;
                       }
                       default:
                         throw new KettleException( "Conversion from Neo4j daa type "+neoType.name()+" to a Kettle Date isn't supported yet" );
                     }
                    } else {
                      LocalDate localDate = recordValue.asLocalDate();
                      value = java.sql.Date.valueOf( localDate );
                    }
                    break;
                  case ValueMetaInterface.TYPE_TIMESTAMP:
                    LocalDateTime localDateTime = recordValue.asLocalDateTime();
                    value = java.sql.Timestamp.valueOf( localDateTime );
                    break;
                  default:
                    throw new KettleException( "Unable to convert Neo4j data to type " + targetValueMeta.toStringMeta() );
                }
              } catch ( Exception e ) {
                throw new KettleException(
                  "Unable to convert Neo4j record value '" + returnValue.getName() + "' to type : " + targetValueMeta.getTypeDesc(), e );
              }
            }
            outputRow[ index++ ] = value;
          }

          // Pass the rows to the next steps
          //
          putRow( data.outputRowMeta, outputRow );
        }
      }

      // Now that all result rows are consumed we can evaluate the result summary.
      //
      if ( processSummary( result ) ) {
        setErrors( 1L );
        stopAll();
        setOutputDone();
        throw new KettleException( "Error found in executing cypher statement" );
      }
    }
  }

  /**
   * Convert the given record value to String.
   * For complex data types it's a conversion to JSON.
   *
   * @param recordValue
   * @param sourceType
   * @return the JSON string
   */
  private String convertToString( Value recordValue, GraphPropertyDataType sourceType ) {
    if (recordValue==null) {
      return null;
    }
    if (sourceType==null) {
      return JSONValue.toJSONString( recordValue.asObject() );
    }
    switch(sourceType){
      case String: return recordValue.asString();
      default: return JSONValue.toJSONString( recordValue.asObject() );
    }
  }

  private boolean processSummary( Result result ) {
    boolean error = false;
    ResultSummary summary = result.consume();
    for ( Notification notification : summary.notifications() ) {
      log.logError( notification.title() + " (" + notification.severity() + ")" );
      log.logError( notification.code() + " : " + notification.description() + ", position " + notification.position() );
      error = true;
    }
    return error;
  }

  @Override public void batchComplete() {

    try {
      wrapUpTransaction();
    } catch(Exception e) {
      setErrors( getErrors()+1 );
      stopAll();
      throw new RuntimeException( "Unable to complete batch of records", e );
    }

  }

  private void wrapUpTransaction() {

    if (!isStopped()) {
      try {
        if ( meta.isUsingUnwind() && data.unwindList != null ) {
          if ( data.unwindList.size() > 0 ) {
            writeUnwindList();
          }
        } else {
          // See if there are statements left to execute...
          //
          if (data.cypherStatements!=null && data.cypherStatements.size()>0) {
            runCypherStatementsBatch();
          }
        }
      } catch ( Exception e ) {
        setErrors( getErrors() + 1 );
        stopAll();
        throw new RuntimeException( "Unable to run batch of cypher statements", e );
      }
    }

    // At the end of each batch, do a commit.
    //
    if ( data.outputCount > 0 ) {

      // With UNWIND we don't have to end a transaction
      //
      if ( data.transaction != null ) {
        if ( getErrors() == 0 ) {
          data.transaction.commit();
        } else {
          data.transaction.rollback();
        }
        data.transaction.close();
      }
      data.outputCount = 0;
    }
  }
}
