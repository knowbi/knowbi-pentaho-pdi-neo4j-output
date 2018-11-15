package bi.know.kettle.neo4j.steps.output;

import bi.know.kettle.neo4j.core.GraphUsage;
import bi.know.kettle.neo4j.core.MetaStoreUtil;
import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import bi.know.kettle.neo4j.steps.BaseNeoStep;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class Neo4JOutput extends BaseNeoStep implements StepInterface {
  private static Class<?> PKG = Neo4JOutput.class; // for i18n purposes, needed by Translator2!!
  private Neo4JOutputMeta meta;
  private Neo4JOutputData data;


  public Neo4JOutput( StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
    super( s, stepDataInterface, c, t, dis );
  }


  /**
   * TODO:
   * 1. option to do NODE CREATE/NODE UPDATE (merge default?)
   * 2. optional commit size
   * 3. option to return node id?
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (Neo4JOutputMeta) smi;
    data = (Neo4JOutputData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
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

      // Create a session
      //
      data.session = data.driver.session();

      if ( row != null ) {
        // Create indexes for the primary properties of the From and To nodes
        //
        if ( meta.isCreatingIndexes() ) {
          try {
            createNodePropertyIndexes( meta, data, getInputRowMeta(), row );
          } catch ( KettleException e ) {
            log.logError( "Unable to create indexes", e );
            return false;
          }
        }
      }
      data.fromUnwindList = new ArrayList<>();
      data.toUnwindList = new ArrayList<>();

    }

    try {
      if ( meta.getFromNodeProps().length > 0 ) {
        if ( meta.isUsingCreate() ) {

          if ( data.fromLabelsClause == null ) {
            data.fromLabelsClause = getLabels( "n", getInputRowMeta(), row, data.fromNodeLabelIndexes );
          }

          createNode( getInputRowMeta(), row, data, data.fromNodeLabelIndexes, data.fromNodePropIndexes, meta.getFromNodePropNames(),
            data.fromNodePropTypes, data.fromLabelsClause, data.fromUnwindList );
          updateUsageMap( getInputRowMeta(), row, data.fromNodeLabelIndexes, GraphUsage.NODE_CREATE );

        } else {
          mergeNode( getInputRowMeta(), row, data, data.fromNodeLabelIndexes, data.fromNodePropIndexes, meta.getFromNodePropNames(),
            data.fromNodePropTypes, meta.getFromNodePropPrimary() );
          updateUsageMap( getInputRowMeta(), row, data.fromNodeLabelIndexes, GraphUsage.NODE_UPDATE );

        }
      }
      if ( meta.getToNodeProps().length > 0 ) {
        if ( meta.isUsingCreate() ) {

          if ( data.toLabelsClause == null ) {
            data.toLabelsClause = getLabels( "n", getInputRowMeta(), row, data.toNodeLabelIndexes );
          }

          createNode( getInputRowMeta(), row, data, data.toNodeLabelIndexes, data.toNodePropIndexes, meta.getToNodePropNames(), data.toNodePropTypes,
            data.toLabelsClause, data.toUnwindList );
          updateUsageMap( getInputRowMeta(), row, data.toNodeLabelIndexes, GraphUsage.NODE_CREATE );

        } else {
          mergeNode( getInputRowMeta(), row, data, data.toNodeLabelIndexes, data.toNodePropIndexes, meta.getToNodePropNames(), data.toNodePropTypes,
            meta.getToNodePropPrimary() );
          updateUsageMap( getInputRowMeta(), row, data.toNodeLabelIndexes, GraphUsage.NODE_UPDATE );
        }
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "Neo4JOutput.addNodeError" ) + e.getMessage(), e );
    }

    try {
      if ( data.relationshipIndex >= 0 ) {
        createRelationship( getInputRowMeta(), row, meta, data );
        updateUsageMap( getInputRowMeta(), row, new int[] { data.relationshipIndex }, GraphUsage.RELATIONSHIP_UPDATE );
      }
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "Neo4JOutput.addRelationshipError" ) + e.getMessage(), e );
    }

    putRow( data.outputRowMeta, row );
    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (Neo4JOutputMeta) smi;
    data = (Neo4JOutputData) sdi;

    // Connect to Neo4j using info in Neo4j JDBC connection metadata...
    //
    if ( StringUtils.isEmpty( meta.getConnection() ) ) {
      log.logError( "You need to specify a Neo4j connection to use in this step" );
      return false;
    }

    try {
      // To correct lazy programmers who built certain PDI steps...
      //
      data.metaStore = MetaStoreUtil.findMetaStore( this );
      data.neoConnection = NeoConnectionUtils.getConnectionFactory( data.metaStore ).loadElement( meta.getConnection() );
      data.neoConnection.initializeVariablesFrom( this );
    } catch ( MetaStoreException e ) {
      log.logError( "Could not load Neo4j connection '" + meta.getConnection() + "' from the metastore", e );
      return false;
    }

    data.batchSize = Const.toLong( environmentSubstitute( meta.getBatchSize() ), 1 );

    try {
      data.driver = data.neoConnection.getDriver( log );
    } catch ( Exception e ) {
      log.logError( "Unable to get or create Neo4j database driver for database '" + data.neoConnection.getName() + "'", e );
      return false;
    }

    return super.init( smi, sdi );
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    data = (Neo4JOutputData) sdi;

    try {
      wrapUpTransaction();
    } catch ( KettleException e ) {
      logError( "Error wrapping up transaction", e );
      setErrors( 1L );
      stopAll();
    }

    if ( data.session != null ) {
      data.session.close();
    }
    if ( data.driver != null ) {
      data.driver.close();
    }

    super.dispose( smi, sdi );
  }

  private void createNode( RowMetaInterface rowMeta, Object[] row, Neo4JOutputData data, int[] nodeLabelIndexes, int[] nodePropIndexes,
                           String[] nodePropNames,
                           GraphPropertyType[] propertyTypes, String labelsClause,
                           List<Map<String, Object>> unwindList ) throws KettleException {

    // Let's use UNWIND by default for now
    //
    Map<String, Object> rowMap = new HashMap<>();

    // Add all the node properties for the current row to the rowMap
    //
    for ( int i = 0; i < nodePropIndexes.length; i++ ) {

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[ i ] );
      Object valueData = row[ nodePropIndexes[ i ] ];

      GraphPropertyType propertyType = propertyTypes[ i ];
      Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

      String propName = "";
      if ( StringUtils.isNotEmpty( nodePropNames[ i ] ) ) {
        propName = nodePropNames[ i ];
      } else {
        propName = valueMeta.getName(); // Take the name from the input field.
      }

      rowMap.put( propName, neoValue );
    }

    // Add the rowMap to the unwindList...
    //
    unwindList.add( rowMap );

    // See if it's time to push the collected data to the database
    //
    if ( unwindList.size() >= data.batchSize ) {

      createNodeEmptyUnwindList( data, unwindList, labelsClause );

    }
  }

  private void createNodeEmptyUnwindList( Neo4JOutputData data, List<Map<String, Object>> unwindList, String labelsClause ) throws KettleException {
    Map<String, Object> properties = Collections.singletonMap( "props", unwindList );

    // Build cypher statement...
    //
    String cypher = "UNWIND $props AS properties CREATE(" + labelsClause + ") SET n = properties";

    if ( log.isDebug() ) {
      logDebug( "Running Cypher: " + cypher );
      logDebug( "properties list size : " + unwindList.size() );
    }

    // Run it always without transactions...
    //
    StatementResult result = data.session.writeTransaction( tx -> tx.run( cypher, properties ) );
    processSummary( result );

    // Clear the list
    //
    unwindList.clear();
  }

  private void mergeNode( RowMetaInterface rowMeta, Object[] row, Neo4JOutputData data, int[] nodeLabelIndexes, int[] nodePropIndexes,
                          String[] nodePropNames, GraphPropertyType[] propertyTypes, boolean[] nodePropPrimary )
    throws KettleException {

    // Add labels
    //
    String labels = getLabels( "n", rowMeta, row, nodeLabelIndexes );

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
    String setCypher = " SET ";
    for ( int i = 0; i < nodePropIndexes.length; i++ ) {
      if ( nodePropPrimary[ i ] ) {
        if ( firstMerge ) {
          firstMerge = false;
        } else {
          mergeCypher += ", ";
        }
      } else {
        if ( firstSet ) {
          firstSet = false;
        } else {
          setCypher += ", ";
        }
      }

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[ i ] );
      Object valueData = row[ nodePropIndexes[ i ] ];

      GraphPropertyType propertyType = propertyTypes[ i ];
      Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

      String propName = "";
      if ( StringUtils.isNotEmpty( nodePropNames[ i ] ) ) {
        propName = nodePropNames[ i ];
      } else {
        propName = valueMeta.getName(); // Take the name from the input field.
      }

      if ( nodePropPrimary[ i ] ) {
        mergeCypher += propName + " : {" + propName + "}";
      } else {
        setCypher += "n." + propName + " = {" + propName + "}";
      }
      parameters.put( propName, neoValue );
    }
    mergeCypher += "}";

    // MERGE (n:Person:Mens:`Human Being` { id: {id} }) ON MATCH SET title = {title} ;
    //
    String stmt = "MERGE (" + labels + mergeCypher + ") ";
    if ( !firstSet ) {
      stmt += setCypher;
    }
    stmt += Const.CR + ";";

    try {

      runStatement( data, stmt, parameters );

    } catch ( Exception e ) {
      logError( "Error executing statement: " + stmt, e );
      setErrors( 1 );
      stopAll();
      setOutputDone();  // signal end to receiver(s)
      throw new KettleStepException( e.getMessage() );
    }
  }

  private String getLabels( String nodeAlias, RowMetaInterface rowMeta, Object[] row, int[] nodeLabelIndexes ) throws KettleValueException {

    String labels = nodeAlias;
    for ( int i = 0; i < nodeLabelIndexes.length; i++ ) {
      labels += ":";
      labels += escapeLabel( rowMeta.getString( row, nodeLabelIndexes[ i ] ) );
    }
    return labels;
  }

  private void runStatement( Neo4JOutputData data, String stmt, Map<String, Object> parameters ) throws KettleException {

    // Execute the cypher with all the parameters...
    //
    if ( log.isDebug() ) {
      logDebug( "Statement: " + stmt );
      logDebug( "Parameters: " + parameters.keySet() );
    }
    StatementResult result;

    if ( data.batchSize <= 1 ) {
      result = data.session.run( stmt, parameters );
    } else {
      if ( data.outputCount == 0 ) {
        data.transaction = data.session.beginTransaction();
      }
      result = data.transaction.run( stmt, parameters );
      data.outputCount++;
      incrementLinesOutput();

      if ( data.outputCount >= data.batchSize ) {
        data.transaction.success();
        data.transaction.close();
        data.outputCount = 0;
      }
    }

    // Evaluate the result, see if there are errors
    //
    processSummary( result );
  }

  private void processSummary( StatementResult result ) throws KettleException {
    boolean error = false;
    ResultSummary summary = result.consume();
    for ( Notification notification : summary.notifications() ) {
      log.logError( notification.title() + " (" + notification.severity() + ")" );
      log.logError( notification.code() + " : " + notification.description() + ", position " + notification.position() );
      error = true;
    }
    if ( error ) {
      throw new KettleException( "Error found while executing cypher statement(s)" );
    }
  }

  private void createRelationship( RowMetaInterface rowMeta, Object[] rowData, Neo4JOutputMeta meta, Neo4JOutputData data ) throws
    KettleException {

    try {

      Map<String, Object> parameters = new HashMap<>();
      AtomicInteger paramNr = new AtomicInteger( 0 );

      // MATCH clause
      //
      String relCypher = "MATCH ";
      relCypher += generateMatchClause( "from",
        meta.getFromNodeLabels(), meta.getFromNodeProps(), meta.getFromNodePropNames(), data.fromNodePropTypes, meta.getFromNodePropPrimary(),
        rowMeta, rowData,
        data.fromNodeLabelIndexes, data.fromNodePropIndexes,
        parameters, paramNr
      );
      relCypher += ", ";
      relCypher += generateMatchClause( "to",
        meta.getToNodeLabels(), meta.getToNodeProps(), meta.getToNodePropNames(), data.toNodePropTypes, meta.getToNodePropPrimary(),
        rowMeta, rowData,
        data.toNodeLabelIndexes, data.toNodePropIndexes,
        parameters,
        paramNr );
      relCypher += Const.CR;
      relCypher += "MERGE (from)-[rel:`" + rowMeta.getString( rowData, data.relationshipIndex ) + "`] -> (to)";
      relCypher += Const.CR;
      if ( meta.getRelProps().length > 0 ) {
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
          relCypher += "rel." + propName + " = {" + parameterName + "}";
          parameters.put( parameterName, neoValue );
        }
      }
      relCypher += Const.CR;
      relCypher += ";";
      relCypher += Const.CR;

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
      throw new KettleException( "Unable to generate relationship Cypher statement : ", e );
    }
  }

  private String generateMatchClause( String alias, String[] nodeLabels, String[] nodeProps, String[] nodePropNames,
                                      GraphPropertyType[] nodePropTypes,
                                      boolean[] nodePropPrimary,
                                      RowMetaInterface rowMeta, Object[] rowData, int[] nodeLabelIndexes, int[] nodePropIndexes,
                                      Map<String, Object> parameters, AtomicInteger paramNr ) throws KettleValueException {
    String matchClause = "(" + alias;
    for ( int i = 0; i < nodeLabels.length; i++ ) {
      String label = escapeProp( rowMeta.getString( rowData, nodeLabelIndexes[ i ] ) );
      matchClause += ":" + label;
    }
    matchClause += " {";

    boolean firstProperty = true;
    for ( int i = 0; i < nodeProps.length; i++ ) {
      if ( nodePropPrimary[ i ] ) {
        if ( firstProperty ) {
          firstProperty = false;
        } else {
          matchClause += ", ";
        }
        String propName;
        if ( StringUtils.isNotEmpty( nodePropNames[ i ] ) ) {
          propName = nodePropNames[ i ];
        } else {
          propName = nodeProps[ i ];
        }

        ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[ i ] );
        Object valueData = rowData[ nodePropIndexes[ i ] ];

        GraphPropertyType propertyType = nodePropTypes[ i ];
        Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

        String parameterName = "param" + paramNr.incrementAndGet();
        matchClause += propName + " : {" + parameterName + "}";

        parameters.put( parameterName, neoValue );
      }
    }
    matchClause += " })";

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

    createIndexForNode( data, meta.getFromNodeLabels(), meta.getFromNodeProps(), meta.getFromNodePropNames(), meta.getFromNodePropPrimary(), rowMeta,
      rowData );
    createIndexForNode( data, meta.getToNodeLabels(), meta.getToNodeProps(), meta.getToNodePropNames(), meta.getToNodePropPrimary(), rowMeta,
      rowData );

    // TODO Check if we need indexes for relationships

  }

  private void createIndexForNode( Neo4JOutputData data, String[] nodeLabels, String[] nodeProps, String[] nodePropNames, boolean[] nodePropPrimary,
                                   RowMetaInterface rowMeta, Object[] rowData )
    throws KettleValueException {

    // Create a index on the primary fields of the node properties
    //
    if ( nodeLabels.length > 0 ) {
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
        NeoConnectionUtils.createNodeIndex( log, data.session, Arrays.asList( nodeLabel ), primaryProperties );
      }
    }
  }

  @Override public void batchComplete() throws KettleException {
    wrapUpTransaction();
  }

  private void wrapUpTransaction() throws KettleException {
    if ( data.fromUnwindList != null && data.fromUnwindList.size() > 0 ) {
      createNodeEmptyUnwindList( data, data.fromUnwindList, data.fromLabelsClause );
    }
    if ( data.toUnwindList != null && data.toUnwindList.size() > 0 ) {
      createNodeEmptyUnwindList( data, data.toUnwindList, data.toLabelsClause );
    }

    // Allow gc
    //
    data.fromUnwindList = null;
    data.toUnwindList = null;

    if ( data.outputCount > 0 ) {
      data.transaction.success();
      data.transaction.close();
      data.outputCount = 0;
    }
  }

  /**
   * Update the usagemap.  Add all the labels to the node usage.
   *
   * @param inputRowMeta
   * @param row
   * @param labelIndexes
   * @param usage
   */
  protected void updateUsageMap( RowMetaInterface inputRowMeta, Object[] row, int[] labelIndexes, GraphUsage usage ) throws KettleValueException {

    Map<String, Set<String>> stepsMap = data.usageMap.get( usage.name() );
    if ( stepsMap == null ) {
      stepsMap = new HashMap<>();
      data.usageMap.put( usage.name(), stepsMap );
    }

    Set<String> labelSet = stepsMap.get( getStepname() );
    if ( labelSet == null ) {
      labelSet = new HashSet<>();
      stepsMap.put( getStepname(), labelSet );
    }

    for ( int labelIndex : labelIndexes ) {
      String label = inputRowMeta.getString( row, labelIndex );
      if ( StringUtils.isNotEmpty( label ) ) {
        labelSet.add( label );
      }
    }
  }
}
