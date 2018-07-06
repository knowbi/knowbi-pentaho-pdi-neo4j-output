package bi.know.kettle.neo4j.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import bi.know.kettle.neo4j.output.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;


public class Neo4JOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = Neo4JOutput.class; // for i18n purposes, needed by Translator2!!


  public Neo4JOutput( StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
    super( s, stepDataInterface, c, t, dis );
  }


  /**
   * TODO:
   * 1. option to do CREATE/MERGE (merge default?)
   * 2. optional commit size
   * 3. option to return node id?
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Neo4JOutputMeta meta = (Neo4JOutputMeta) smi;
    Neo4JOutputData data = (Neo4JOutputData) sdi;

    Object[] row = getRow();

    if ( first ) {
      first = false;

      data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
      data.fieldNames = data.outputRowMeta.getFieldNames();
      data.fromNodePropIndexes = new int[ meta.getFromNodeProps().length ];
      data.fromNodePropTypes = new GraphPropertyType[ meta.getFromNodeProps().length ];
      for ( int i = 0; i < meta.getFromNodeProps().length; i++ ) {
        data.fromNodePropIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getFromNodeProps()[ i ] );
        data.fromNodePropTypes[ i ] = GraphPropertyType.parseCode( meta.getFromNodePropTypes()[ i ] );
      }
      data.fromNodeLabelIndexes = new int[ meta.getFromNodeLabels().length ];
      for ( int i = 0; i < meta.getFromNodeLabels().length; i++ ) {
        data.fromNodeLabelIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getFromNodeLabels()[ i ] );
      }
      data.toNodePropIndexes = new int[ meta.getToNodeProps().length ];
      data.toNodePropTypes = new GraphPropertyType[ meta.getToNodeProps().length ];
      for ( int i = 0; i < meta.getToNodeProps().length; i++ ) {
        data.toNodePropIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getToNodeProps()[ i ] );
        data.toNodePropTypes[ i ] = GraphPropertyType.parseCode( meta.getToNodePropTypes()[ i ] );
      }
      data.toNodeLabelIndexes = new int[ meta.getToNodeLabels().length ];
      for ( int i = 0; i < meta.getToNodeLabels().length; i++ ) {
        data.toNodeLabelIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getToNodeLabels()[ i ] );
      }
      data.relPropIndexes = new int[ meta.getRelProps().length ];
      data.relPropTypes = new GraphPropertyType[ meta.getRelProps().length ];
      for ( int i = 0; i < meta.getRelProps().length; i++ ) {
        data.relPropIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getRelProps()[ i ] );
        data.relPropTypes[ i ] = GraphPropertyType.parseCode( meta.getRelPropTypes()[ i ] );
      }
      data.relationshipIndex = data.outputRowMeta.indexOfValue( meta.getRelationship() );

      if (row!=null) {
        // Create indexes for the primary properties of the From and To nodes
        //
        if (meta.isCreatingIndexes()) {
          try {
            createNodePropertyIndexes(meta, data, getInputRowMeta(), row);
          } catch ( KettleException e ) {
            log.logError( "Unable to create indexes", e );
            return false;
          }
        }
      }
    }

    if ( row == null ) {
      setOutputDone();
      return false;
    }

    try {
      if ( meta.getFromNodeProps().length > 0 ) {
        createNode( getInputRowMeta(), row, data, data.fromNodeLabelIndexes, data.fromNodePropIndexes, meta.getFromNodePropNames(),
          data.fromNodePropTypes, meta.getFromNodePropPrimary() );
      }
      if ( meta.getToNodeProps().length > 0 ) {
        createNode( getInputRowMeta(), row, data, data.toNodeLabelIndexes, data.toNodePropIndexes, meta.getToNodePropNames(), data.toNodePropTypes,
          meta.getToNodePropPrimary() );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "Neo4JOutput.addNodeError" ) + e.getMessage(), e );
    }

    try {
      if (data.relationshipIndex>=0) {
        createRelationship( getInputRowMeta(), row, meta, data );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "Neo4JOutput.addRelationshipError" ) + e.getMessage(), e );
    }

    putRow( data.outputRowMeta, row );
    return true;
  }


  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    Neo4JOutputMeta meta = (Neo4JOutputMeta) smi;
    Neo4JOutputData data = (Neo4JOutputData) sdi;

    // Connect to Neo4j using info in Neo4j JDBC connection metadata...
    //
    try {
      data.neoConnection = NeoConnectionUtils.getConnectionFactory( metaStore ).loadElement( meta.getConnection() );
    } catch ( MetaStoreException e ) {
      log.logError("Could not load Neo4j connection '"+meta.getConnection()+"' from the metastore", e);
      return false;
    }

    data.batchSize = Const.toLong(environmentSubstitute( meta.getBatchSize()), 1);

    try {
      data.driver = data.neoConnection.getDriver(log);
    } catch(Exception e) {
      log.logError( "Unable to get or create Neo4j database driver for database '"+ data.neoConnection.getName()+"'", e);
      return false;
    }

    // Create a session
    //
    data.session = data.driver.session();

    return super.init( smi, sdi );
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    Neo4JOutputData data = (Neo4JOutputData) sdi;

    if ( data.outputCount >0) {
      data.transaction.success();
      data.transaction.close();
    }
    if ( data.session!=null) {
      data.session.close();
    }
    data.driver.close();

    super.dispose( smi, sdi );
  }


  private void createNode( RowMetaInterface rowMeta, Object[] row, Neo4JOutputData data, int[] nodeLabelIndexes, int[] nodePropIndexes,
                           String[] nodePropNames, GraphPropertyType[] propertyTypes, boolean[] nodePropPrimary )
    throws KettleException {

    // Add labels
    //
    String labels = "n:";
    for ( int i = 0; i < nodeLabelIndexes.length; i++ ) {
      String label = escapeLabel( rowMeta.getString( row, nodeLabelIndexes[i] ) );
      labels += label;
      if ( i != ( nodeLabelIndexes.length ) - 1 ) {
        labels += ":";
      }
    }

    // Add primary properties
    //
    // Examples:
    //
    //   { name: 'Andres', title: 'Developer' }
    //
    //   { id: 12345 }
    //   ON MATCH SET node.firstName = {firstName}, node.lastName = {lastName}
    //
    //
    Map<String, Object> parameters = new HashMap<String, Object>();
    boolean firstSet = true;
    boolean firstMerge = true;
    String mergeCypher = " { ";
    String setCypher = " ON MATCH SET ";
    for ( int i = 0; i < nodePropIndexes.length; i++ ) {
      if (nodePropPrimary[i]) {
        if (firstMerge) {
          firstMerge = false;
        } else {
          mergeCypher+= ", ";
        }
      } else {
        if (firstSet) {
          firstSet = false;
        } else {
          setCypher+= ", ";
        }
      }

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[i] );
      Object valueData = row[nodePropIndexes[i]];

      GraphPropertyType propertyType = propertyTypes[i];
      Object neoValue = propertyType.convertFromKettle( valueMeta, valueData);

      String propName = "";
      if ( StringUtils.isNotEmpty(nodePropNames[ i ]) ) {
        propName = nodePropNames[ i ];
      } else {
        propName = valueMeta.getName(); // Take the name from the input field.
      }

      if (nodePropPrimary[i]) {
        mergeCypher += propName + " : {" + propName + "}";
      } else {
        setCypher += "n."+propName + " = {" + propName + "}";
      }
      parameters.put( propName, neoValue );
    }
    mergeCypher += "}";

    // MERGE (n:Person:Mens:`Human Being` { id: {id} }) ON MATCH SET title = {title} ;
    //
    String stmt = "MERGE (" + labels + mergeCypher + ") ";
    if (!firstSet) {
      stmt+=setCypher;
    }
    stmt+=Const.CR+";";

    try {

      runStatement(data, stmt, parameters);

    } catch ( Exception e ) {
      logError( "Error executing statement: " + stmt, e );
      setErrors( 1 );
      stopAll();
      setOutputDone();  // signal end to receiver(s)
      throw new KettleStepException( e.getMessage() );
    }
  }

  private void runStatement( Neo4JOutputData data, String stmt, Map<String,Object> parameters ) {

    // Execute the cypher with all the parameters...
    //
    if (log.isDebug()) {
      logDebug( "Statement: " + stmt );
      logDebug( "Parameters: " + parameters.keySet() );
    }

    if (data.batchSize<=1) {
      data.session.run( stmt, parameters );
    } else {
      if (data.outputCount ==0) {
        data.transaction = data.session.beginTransaction();
      }
      data.transaction.run( stmt, parameters );
      data.outputCount++;
      incrementLinesOutput();

      if (data.outputCount >=data.batchSize) {
        data.transaction.success();
        data.transaction.close();
        data.outputCount =0;
      }
    }
  }

  private void createRelationship(RowMetaInterface rowMeta, Object[] rowData, Neo4JOutputMeta meta, Neo4JOutputData data) throws
    KettleException {

    try {

      Map<String, Object> parameters = new HashMap<>();
      AtomicInteger paramNr = new AtomicInteger(0);

      // MATCH clause
      //
      String relCypher = "MATCH ";
      relCypher+=generateMatchClause("from",
        meta.getFromNodeLabels(), meta.getFromNodeProps(), meta.getFromNodePropNames(), data.fromNodePropTypes, meta.getFromNodePropPrimary(),
        rowMeta, rowData,
        data.fromNodeLabelIndexes, data.fromNodePropIndexes,
        parameters, paramNr
      );
      relCypher+=", ";
      relCypher+=generateMatchClause("to",
        meta.getToNodeLabels(), meta.getToNodeProps(), meta.getToNodePropNames(), data.toNodePropTypes, meta.getToNodePropPrimary(),
        rowMeta, rowData,
        data.toNodeLabelIndexes, data.toNodePropIndexes,
        parameters,
        paramNr );
      relCypher+=Const.CR;
      relCypher+="MERGE (from)-[rel:`" + String.valueOf( rowData[ data.relationshipIndex ] ) + "`] -> (to)";
      relCypher+=Const.CR;
      if (meta.getRelProps().length>0) {
        relCypher += "SET ";
        for ( int i = 0; i < meta.getRelProps().length; i++ ) {
          if ( i > 0 ) {
            relCypher += ", ";
          }

          String propName;
          if ( StringUtils.isNotEmpty( meta.getRelPropNames()[ i ] ) ) {
            propName = meta.getRelPropNames()[ i ];
          } else {
            propName = meta.getRelProps()[ i ];
          }

          ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.relPropIndexes[ i ] );
          Object valueData = rowData[ data.relPropIndexes[ i ] ];

          GraphPropertyType propertyType = data.relPropTypes[ i ];
          Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

          String parameterName = "param" + paramNr.incrementAndGet();
          relCypher += "rel." + propName + " = {"+parameterName+ "}";
          parameters.put( parameterName, neoValue );
        }
      }
      relCypher+=Const.CR;
      relCypher+=";";
      relCypher+=Const.CR;

      try {

        runStatement( data, relCypher, parameters );

      } catch ( Exception e ) {
        logError( "Error executing statement: " + relCypher, e );
        setErrors( 1 );
        stopAll();
        setOutputDone();  // signal end to receiver(s)
        throw new KettleStepException( e.getMessage() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to generate relationship Cypher statement : ", e);
    }
  }

  private String generateMatchClause( String alias, String[] nodeLabels, String[] nodeProps, String[] nodePropNames,
                                      GraphPropertyType[] nodePropTypes,
                                      boolean[] nodePropPrimary,
                                      RowMetaInterface rowMeta, Object[] rowData, int[] nodeLabelIndexes, int[] nodePropIndexes,
                                      Map<String, Object> parameters, AtomicInteger paramNr ) throws KettleValueException {
    String matchClause="("+alias;
    for ( int i = 0; i < nodeLabels.length; i++ ) {
      String label = escapeProp( rowMeta.getString(rowData, nodeLabelIndexes[i])  );
      matchClause+=":"+label;
    }
    matchClause+=" {";

    boolean firstProperty = true;
    for ( int i = 0; i < nodeProps.length; i++ ) {
      if ( nodePropPrimary[ i ] ) {
        if (firstProperty) {
          firstProperty=false;
        } else {
          matchClause+=", ";
        }
        String propName;
        if (StringUtils.isNotEmpty( nodePropNames[i])) {
          propName=nodePropNames[i];
        } else {
          propName=nodeProps[i];
        }

        ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[i] );
        Object valueData = rowData[nodePropIndexes[i]];

        GraphPropertyType propertyType = nodePropTypes[i];
        Object neoValue = propertyType.convertFromKettle( valueMeta, valueData);

        String parameterName = "param"+paramNr.incrementAndGet();
        matchClause+= propName + " : {" + parameterName+ "}";

        parameters.put( parameterName, neoValue );
      }
    }
    matchClause+=" })";

    return matchClause;
  }


  public String escapeLabel( String str ) {
    if ( str.contains( " " ) || str.contains( "." ) ) {
      str = "`" + str + "`";
    }
    return str;
  }

  public String escapeProp( String str ) {
    return StringEscapeUtils.escapeJava( str );
  }

  private void createNodePropertyIndexes( Neo4JOutputMeta meta, Neo4JOutputData data, RowMetaInterface rowMeta, Object[] rowData )
    throws KettleException {

    // Only try to create an index on the first step copy
    //
    if (getCopy()>0) {
      return;
    }

    createIndexForNode(data, meta.getFromNodeLabels(), meta.getFromNodeProps(), meta.getFromNodePropNames(), meta.getFromNodePropPrimary(), rowMeta, rowData);
    createIndexForNode(data, meta.getToNodeLabels(), meta.getToNodeProps(), meta.getToNodePropNames(), meta.getToNodePropPrimary(), rowMeta, rowData);

    // TODO Check if we need indexes for relationships

  }

  private void createIndexForNode( Neo4JOutputData data, String[] nodeLabels, String[] nodeProps, String[] nodePropNames, boolean[] nodePropPrimary, RowMetaInterface rowMeta, Object[] rowData )
    throws KettleValueException {

    // Create a index on the primary fields of the node properties
    //
    if (nodeLabels.length>0) {
      String nodeLabelField = nodeLabels[ 0 ];

      String nodeLabel = rowMeta.getString( rowData, nodeLabelField, null );

      List<String> primaryProperties = new ArrayList<>();
      for ( int f = 0; f < nodeProps.length; f++ ) {
        if ( nodePropPrimary[ f ] ) {
          if ( StringUtils.isNotEmpty( nodePropNames[ f ] ) ) {
            primaryProperties.add( nodePropNames[ f ] );
          } else {
            primaryProperties.add( nodeProps[ f ] );
          }
        }
      }

      if ( nodeLabel != null && primaryProperties.size() > 0 ) {
        createIndex( data, nodeLabel, primaryProperties );
      }
    }
  }

  private void createIndex( Neo4JOutputData data, String nodeLabel, List<String> primaryProperties ) {
    // CREATE INDEX ON :NodeLabel(property1, property2);
    //
    String indexCypher = "CREATE INDEX ON ";
    String labelsClause = ":" + nodeLabel;

    indexCypher += labelsClause;
    indexCypher += "( ";
    for (int i=0;i<primaryProperties.size();i++) {
      if ( i > 0 ) {
        indexCypher += ", ";
      }
      indexCypher += primaryProperties.get( i );
    }
    indexCypher+=" );";

    logBasic( "Creating index : " + indexCypher );

    data.session.run( indexCypher );
  }
}
