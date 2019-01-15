package bi.know.kettle.neo4j.steps.output;

import bi.know.kettle.neo4j.core.GraphUsage;
import bi.know.kettle.neo4j.core.MetaStoreUtil;
import bi.know.kettle.neo4j.core.data.GraphData;
import bi.know.kettle.neo4j.core.data.GraphNodeData;
import bi.know.kettle.neo4j.core.data.GraphPropertyData;
import bi.know.kettle.neo4j.core.data.GraphPropertyDataType;
import bi.know.kettle.neo4j.core.data.GraphRelationshipData;
import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.DriverSingleton;
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
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
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
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

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
        if ( data.fromNodeLabelIndexes[ i ] < 0 && StringUtils.isEmpty( meta.getFromNodeLabelValues()[ i ] ) ) {
          throw new KettleException( "From node : please provide either a static label value or a field name to determine the label" );
        }
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
        if ( data.toNodeLabelIndexes[ i ] < 0 && StringUtils.isEmpty( meta.getToNodeLabelValues()[ i ] ) ) {
          throw new KettleException( "To node : please provide either a static label value or a field name to determine the label" );
        }
      }
      data.relPropIndexes = new int[ meta.getRelProps().length ];
      data.relPropTypes = new GraphPropertyType[ meta.getRelProps().length ];
      for ( int i = 0; i < meta.getRelProps().length; i++ ) {
        data.relPropIndexes[ i ] = data.outputRowMeta.indexOfValue( meta.getRelProps()[ i ] );
        data.relPropTypes[ i ] = GraphPropertyType.parseCode( meta.getRelPropTypes()[ i ] );
      }
      data.relationshipIndex = data.outputRowMeta.indexOfValue( meta.getRelationship() );
      data.fromLabelValues = new String[ meta.getFromNodeLabelValues().length ];
      for ( int i = 0; i < meta.getFromNodeLabelValues().length; i++ ) {
        data.fromLabelValues[ i ] = environmentSubstitute( meta.getFromNodeLabelValues()[ i ] );
      }
      data.toLabelValues = new String[ meta.getToNodeLabelValues().length ];
      for ( int i = 0; i < meta.getToNodeLabelValues().length; i++ ) {
        data.toLabelValues[ i ] = environmentSubstitute( meta.getToNodeLabelValues()[ i ] );
      }
      data.relationshipLabelValue = environmentSubstitute( meta.getRelationshipValue() );

      data.fromUnwindList = new ArrayList<>();
      data.toUnwindList = new ArrayList<>();
      data.relUnwindList = new ArrayList<>();

      // Create a session
      //
      if (!meta.isReturningGraph()) {
        data.session = data.driver.session();

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
      } else {
        if ( meta.isReturningGraph() ) {
          log.logBasic( "Writing to output graph field, not to Neo4j" );
        }
      }
    }

    if ( meta.isReturningGraph() ) {
      // Let the next steps handle writing to Neo4j
      //
      outputGraphValue( getInputRowMeta(), row );

    } else {

      try {
        if ( meta.getFromNodeProps().length > 0 && !meta.isOnlyCreatingRelationships() ) {

          List<String> fromLabels = getNodeLabels( meta.getFromNodeLabels(), data.fromLabelValues, getInputRowMeta(), row, data.fromNodeLabelIndexes );

          if ( meta.isUsingCreate() ) {

            if ( data.fromLabelsClause == null ) {
              data.fromLabelsClause = getLabels( "n", fromLabels );
            }

            createNode( getInputRowMeta(), row, data, data.fromNodePropIndexes, meta.getFromNodePropNames(),
              data.fromNodePropTypes, data.fromLabelsClause, data.fromUnwindList );
            updateUsageMap( fromLabels, GraphUsage.NODE_CREATE );

          } else {

            if (!meta.isReadOnlyFromNode()) {
              mergeNode( getInputRowMeta(), row, data, fromLabels, data.fromNodePropIndexes, meta.getFromNodePropNames(),
                data.fromNodePropTypes, meta.getFromNodePropPrimary() );
              updateUsageMap( fromLabels, GraphUsage.NODE_UPDATE );
            }

          }
        }
        if ( meta.getToNodeProps().length > 0 && !meta.isOnlyCreatingRelationships() ) {

          List<String> toLabels = getNodeLabels( meta.getToNodeLabels(), data.toLabelValues, getInputRowMeta(), row, data.toNodeLabelIndexes );

          if ( meta.isUsingCreate() ) {

            if ( data.toLabelsClause == null ) {
              data.toLabelsClause = getLabels( "n", toLabels );
            }

            createNode( getInputRowMeta(), row, data, data.toNodePropIndexes, meta.getToNodePropNames(), data.toNodePropTypes,
              data.toLabelsClause, data.toUnwindList );
            updateUsageMap( toLabels, GraphUsage.NODE_CREATE );

          } else {

            if (!meta.isReadOnlyToNode()) {
              mergeNode( getInputRowMeta(), row, data, toLabels, data.toNodePropIndexes, meta.getToNodePropNames(), data.toNodePropTypes,
                meta.getToNodePropPrimary() );
              updateUsageMap( toLabels, GraphUsage.NODE_UPDATE );
            }
          }
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "Neo4JOutput.addNodeError" ) + e.getMessage(), e );
        setErrors( 1 );
        stopAll();
        return false;
      }

      try {

        String relationshipLabel = null;
        if ( data.relationshipIndex >= 0 ) {
          relationshipLabel = getInputRowMeta().getString( row, data.relationshipIndex );
        }
        if ( StringUtils.isEmpty( relationshipLabel ) && StringUtils.isNotEmpty( data.relationshipLabelValue ) ) {
          relationshipLabel = data.relationshipLabelValue;
        }

        // We only create a relationship if we have a label
        //
        if ( StringUtils.isNotEmpty( relationshipLabel ) ) {

          if ( meta.isOnlyCreatingRelationships() ) {
            // Use UNWIND statements to create relationships...
            //
            createOnlyRelationship( getInputRowMeta(), row, meta, data, relationshipLabel );
            updateUsageMap( Collections.singletonList( relationshipLabel ), GraphUsage.RELATIONSHIP_CREATE );

          } else {
            createRelationship( getInputRowMeta(), row, meta, data, relationshipLabel );
            updateUsageMap( Collections.singletonList( relationshipLabel ), GraphUsage.RELATIONSHIP_UPDATE );
          }

        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "Neo4JOutput.addRelationshipError" ) + e.getMessage(), e );
        setErrors( 1 );
        stopAll();
        return false;
      }

      putRow( data.outputRowMeta, row );
    }
    return true;
  }

  private void outputGraphValue( RowMetaInterface rowMeta, Object[] row ) throws KettleException {

    try {

      GraphData graphData = new GraphData();
      GraphNodeData sourceNodeData = null;
      GraphNodeData targetNodeData = null;
      GraphRelationshipData relationshipData = null;

      if ( meta.getFromNodeProps().length > 0 ) {
        sourceNodeData = createGraphNodeData( rowMeta, row, meta.getFromNodeLabels(), data.fromLabelValues, data.fromNodeLabelIndexes,
          data.fromNodePropIndexes, meta.getFromNodePropNames(), meta.getFromNodePropPrimary() );
        if ( !meta.isOnlyCreatingRelationships() ) {
          graphData.getNodes().add( sourceNodeData );
        }
      }
      if ( meta.getToNodeProps().length > 0 ) {
        targetNodeData = createGraphNodeData( rowMeta, row, meta.getToNodeLabels(), data.toLabelValues, data.toNodeLabelIndexes,
          data.toNodePropIndexes, meta.getToNodePropNames(), meta.getToNodePropPrimary() );
        if ( !meta.isOnlyCreatingRelationships()) {
          graphData.getNodes().add( targetNodeData );
        }
      }

      String relationshipLabel = null;
      if ( data.relationshipIndex >= 0 ) {
        relationshipLabel = getInputRowMeta().getString( row, data.relationshipIndex );
      }
      if ( StringUtil.isEmpty( relationshipLabel ) && StringUtils.isNotEmpty( data.relationshipLabelValue ) ) {
        relationshipLabel = data.relationshipLabelValue;
      }
      if ( sourceNodeData != null && targetNodeData != null && StringUtils.isNotEmpty( relationshipLabel ) ) {

        relationshipData = new GraphRelationshipData();
        relationshipData.setSourceNodeId( sourceNodeData.getId() );
        relationshipData.setTargetNodeId( targetNodeData.getId() );
        relationshipData.setLabel( relationshipLabel );
        relationshipData.setId( sourceNodeData.getId() + " -> " + targetNodeData.getId() );

        // Add relationship properties...
        //
        // Set the properties
        //
        for ( int i = 0; i < data.relPropIndexes.length; i++ ) {

          ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.relPropIndexes[ i ] );
          Object valueData = row[ data.relPropIndexes[ i ] ];

          String propertyName = meta.getRelPropNames()[ i ];
          GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromKettle( valueMeta );
          Object propertyNeoValue = propertyType.convertFromKettle( valueMeta, valueData );
          boolean propertyPrimary = false;

          relationshipData.getProperties().add(
            new GraphPropertyData( propertyName, propertyNeoValue, propertyType, propertyPrimary )
          );
        }

        graphData.getRelationships().add( relationshipData );
      }

      // Pass it forward...
      //
      Object[] outputRowData = RowDataUtil.createResizedCopy( row, data.outputRowMeta.size() );
      int startIndex = rowMeta.size();
      outputRowData[ rowMeta.size() ] = graphData;
      putRow( data.outputRowMeta, outputRowData );

    } catch ( Exception e ) {
      throw new KettleException( "Unable to calculate graph output value", e );
    }
  }

  private GraphNodeData createGraphNodeData( RowMetaInterface rowMeta, Object[] row, String[] nodeLabels, String[] nodeLabelValues, int[] nodeLabelIndexes,
                                             int[] nodePropIndexes, String[] nodePropNames, boolean[] nodePropPrimary ) throws KettleException {
    GraphNodeData nodeData = new GraphNodeData();

    // Set the label(s)
    //
    List<String> labels = getNodeLabels( nodeLabels, nodeLabelValues, rowMeta, row, nodeLabelIndexes );
    for ( String label : labels ) {
      nodeData.getLabels().add( label );
    }

    StringBuilder nodeId = new StringBuilder();

    // Set the properties
    //
    for ( int i = 0; i < nodePropIndexes.length; i++ ) {

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[ i ] );
      Object valueData = row[ nodePropIndexes[ i ] ];

      String propertyName = nodePropNames[ i ];
      GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromKettle( valueMeta );
      Object propertyNeoValue = propertyType.convertFromKettle( valueMeta, valueData );
      boolean propertyPrimary = nodePropPrimary[i];

      nodeData.getProperties().add( new GraphPropertyData( propertyName, propertyNeoValue, propertyType, propertyPrimary ) );

      // Part of the key...
      if ( nodePropPrimary[ i ] ) {
        if ( nodeId.length() > 0 ) {
          nodeId.append( "-" );
        }
        nodeId.append( valueMeta.getString( valueData ) );
      }
    }

    if ( nodeId.length() > 0 ) {
      nodeData.setId( nodeId.toString() );
    }


    return nodeData;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (Neo4JOutputMeta) smi;
    data = (Neo4JOutputData) sdi;

    if (!meta.isReturningGraph()) {

      // Connect to Neo4j using info metastore Neo4j Connection metadata
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
        data.driver = DriverSingleton.getDriver( log, data.neoConnection );
      } catch ( Exception e ) {
        log.logError( "Unable to get or create Neo4j database driver for database '" + data.neoConnection.getName() + "'", e );
        return false;
      }
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

    super.dispose( smi, sdi );
  }

  private void createNode( RowMetaInterface rowMeta, Object[] row, Neo4JOutputData data,
                           int[] nodePropIndexes, String[] nodePropNames,
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

  private void mergeNode( RowMetaInterface rowMeta, Object[] row, Neo4JOutputData data,
                          List<String> nodeLabels, int[] nodePropIndexes,
                          String[] nodePropNames, GraphPropertyType[] propertyTypes, boolean[] nodePropPrimary )
    throws KettleException {

    // Add labels
    //
    String labels = getLabels( "n", nodeLabels );

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
    StringBuilder mergeCypher = new StringBuilder( " { " );
    StringBuilder setCypher = new StringBuilder( " SET " );
    for ( int i = 0; i < nodePropIndexes.length; i++ ) {
      if ( nodePropPrimary[ i ] ) {
        if ( firstMerge ) {
          firstMerge = false;
        } else {
          mergeCypher.append( ", " );
        }
      } else {
        if ( firstSet ) {
          firstSet = false;
        } else {
          setCypher.append( ", " );
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
        mergeCypher.append( propName ).append( " : {" ).append( propName ).append( "}" );
      } else {
        setCypher.append( "n." ).append( propName ).append( " = {" ).append( propName ).append( "}" );
      }
      parameters.put( propName, neoValue );
    }
    mergeCypher.append( "}" );

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

  private String getLabels( String nodeAlias, List<String> nodeLabels ) {

    String labels = nodeAlias;
    for ( int i = 0; i < nodeLabels.size(); i++ ) {
      labels += ":";
      labels += escapeLabel( nodeLabels.get( i ) );
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

  private String generateMatchClause( String alias, String mapName, List<String> nodeLabels, String[] nodeProps, String[] nodePropNames,
                                      GraphPropertyType[] nodePropTypes,
                                      boolean[] nodePropPrimary,
                                      RowMetaInterface rowMeta, Object[] rowData, int[] nodePropIndexes,
                                      Map<String, Object> parameters, AtomicInteger paramNr ) throws KettleValueException {
    String matchClause = "(" + alias;
    for ( int i = 0; i < nodeLabels.size(); i++ ) {
      String label = escapeProp( nodeLabels.get( i ) );
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
        String parameterName = "param" + paramNr.incrementAndGet();

        if ( mapName == null ) {
          matchClause += propName + " : {" + parameterName + "}";
        } else {
          matchClause += propName + " : " + mapName + "." + parameterName;
        }

        if ( parameters != null ) {
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( nodePropIndexes[ i ] );
          Object valueData = rowData[ nodePropIndexes[ i ] ];

          GraphPropertyType propertyType = nodePropTypes[ i ];
          Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

          parameters.put( parameterName, neoValue );
        }
      }
    }
    matchClause += " })";

    return matchClause;
  }

  public List<String> getNodeLabels( String[] labelFields, String[] labelValues, RowMetaInterface rowMeta, Object[] rowData, int[] labelIndexes ) throws KettleValueException {
    List<String> labels = new ArrayList<>();

    for ( int a = 0; a < labelFields.length; a++ ) {
      String label = null;
      if ( StringUtils.isNotEmpty( labelFields[ a ] ) ) {
        label = rowMeta.getString( rowData, labelIndexes[ a ] );
      }
      if ( StringUtils.isEmpty( label ) && StringUtils.isNotEmpty( labelValues[ a ] ) ) {
        label = labelValues[ a ];
      }
      if ( StringUtils.isNotEmpty( label ) ) {
        labels.add( label );
      }
    }
    return labels;
  }

  private void createRelationship( RowMetaInterface rowMeta, Object[] rowData, Neo4JOutputMeta meta, Neo4JOutputData data, String relationshipLabel ) throws
    KettleException {

    try {

      Map<String, Object> parameters = new HashMap<>();
      AtomicInteger paramNr = new AtomicInteger( 0 );

      // MATCH clause
      //
      StringBuffer relCypher = new StringBuffer();
      relCypher.append( "MATCH " );

      List<String> fromNodeLabels = getNodeLabels( meta.getFromNodeLabels(), data.fromLabelValues, rowMeta, rowData, data.fromNodeLabelIndexes );

      relCypher.append( generateMatchClause( "from", null, fromNodeLabels,
        meta.getFromNodeProps(), meta.getFromNodePropNames(), data.fromNodePropTypes, meta.getFromNodePropPrimary(),
        rowMeta, rowData,
        data.fromNodePropIndexes,
        parameters, paramNr
      ) );

      relCypher.append( ", " );

      List<String> toNodeLabels = getNodeLabels( meta.getToNodeLabels(), data.toLabelValues, rowMeta, rowData, data.toNodeLabelIndexes );

      relCypher.append( generateMatchClause( "to", null, toNodeLabels,
        meta.getToNodeProps(), meta.getToNodePropNames(), data.toNodePropTypes, meta.getToNodePropPrimary(),
        rowMeta, rowData,
        data.toNodePropIndexes,
        parameters,
        paramNr ) );
      relCypher.append( Const.CR );

      relCypher.append( "MERGE (from)-[rel:`" + relationshipLabel + "`] -> (to) " );
      relCypher.append( Const.CR );
      if ( meta.getRelProps().length > 0 ) {
        relCypher.append( "SET " );
        for ( int i = 0; i < meta.getRelProps().length; i++ ) {
          if ( i > 0 ) {
            relCypher.append( ", " );
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
          relCypher.append( "rel." + propName + " = {" + parameterName + "}" );
          parameters.put( parameterName, neoValue );
        }
      }
      relCypher.append( Const.CR );
      relCypher.append( ";" );
      relCypher.append( Const.CR );

      try {

        runStatement( data, relCypher.toString(), parameters );

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

  private void createOnlyRelationship( RowMetaInterface rowMeta, Object[] rowData, Neo4JOutputMeta meta, Neo4JOutputData data, String relationshipLabel ) throws KettleException {

    try {

      Map<String, Object> parameters = new HashMap<>();
      AtomicInteger paramNr = new AtomicInteger( 0 );

      // Collect all parameters.
      // First the "from" primary fields
      // Then the "to" primary fields
      // Finally the relationship fields
      //

      // FROM
      //
      for ( int i = 0; i < meta.getFromNodePropNames().length; i++ ) {

        if ( meta.getFromNodePropPrimary()[ i ] ) {
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.fromNodePropIndexes[ i ] );
          Object valueData = rowData[ data.fromNodePropIndexes[ i ] ];

          GraphPropertyType propertyType = data.fromNodePropTypes[ i ];
          Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

          // Store this in the map
          //
          String parameterName = "param" + paramNr.incrementAndGet();
          parameters.put( parameterName, neoValue );
        }
      }

      // TO
      //
      for ( int i = 0; i < meta.getToNodePropNames().length; i++ ) {

        if ( meta.getToNodePropPrimary()[ i ] ) {
          ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.toNodePropIndexes[ i ] );
          Object valueData = rowData[ data.toNodePropIndexes[ i ] ];

          GraphPropertyType propertyType = data.toNodePropTypes[ i ];
          Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

          // Store this in the map
          //
          String parameterName = "param" + paramNr.incrementAndGet();
          parameters.put( parameterName, neoValue );
        }
      }

      // Relationship properties
      //
      for ( int i = 0; i < meta.getRelPropNames().length; i++ ) {

        ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.relPropIndexes[ i ] );
        Object valueData = rowData[ data.relPropIndexes[ i ] ];

        GraphPropertyType propertyType = data.relPropTypes[ i ];
        Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

        // Store this in the map
        //
        String parameterName = "param" + paramNr.incrementAndGet();
        parameters.put( parameterName, neoValue );
      }

      // Add it to the unwind list...
      //
      data.relUnwindList.add( parameters );

      if ( data.relUnwindList.size() >= data.batchSize ) {
        emptyRelationshipsUnwindList();
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unable to generate relationship Cypher statement : ", e );
    }
  }

  private void emptyRelationshipsUnwindList() throws KettleException {

    Map<String, Object> properties = Collections.singletonMap( "props", data.relUnwindList );

    AtomicInteger paramNr = new AtomicInteger( 0 );

    // Build cypher statement...
    //
    StringBuilder cypher = new StringBuilder();
    cypher.append( "UNWIND $props AS prop " );
    cypher.append( "MATCH " );

    List<String> fromNodeLabels = getNodeLabels( meta.getFromNodeLabels(), data.fromLabelValues, null, null, null );
    cypher.append( generateMatchClause( "from", "prop", fromNodeLabels, meta.getFromNodeProps(), meta.getFromNodePropNames(), data.fromNodePropTypes, meta.getFromNodePropPrimary(),
      null, null,
      data.fromNodePropIndexes,
      null, paramNr
    ) );
    cypher.append( ", " );

    List<String> toNodeLabels = getNodeLabels( meta.getToNodeLabels(), data.toLabelValues, null, null, null );

    cypher.append( generateMatchClause( "to", "prop", toNodeLabels, meta.getToNodeProps(), meta.getToNodePropNames(), data.toNodePropTypes, meta.getToNodePropPrimary(),
      null, null,
      data.toNodePropIndexes,
      null, paramNr ) );

    cypher.append( Const.CR );

    String relationshipLabel;
    if ( StringUtils.isEmpty( meta.getRelationship() ) ) {
      relationshipLabel = environmentSubstitute( meta.getRelationshipValue() );
    } else {
      throw new KettleException( "We need a static relationship label to create relationships" );
    }

    cypher.append( "CREATE (from)-[rel:`" + relationshipLabel + "`] -> (to)" );
    cypher.append( Const.CR );
    if ( meta.getRelProps().length > 0 ) {
      cypher.append( "SET " );
      for ( int i = 0; i < meta.getRelProps().length; i++ ) {
        if ( i > 0 ) {
          cypher.append( ", " );
        }

        String propName;
        if ( StringUtils.isNotEmpty( meta.getRelPropNames()[ i ] ) ) {
          propName = meta.getRelPropNames()[ i ];
        } else {
          propName = meta.getRelProps()[ i ];
        }

        String parameterName = "param" + paramNr.incrementAndGet();
        cypher.append( "rel." + propName + " = prop." + parameterName );
      }
    }

    if ( log.isDebug() ) {
      logDebug( "Running Cypher: " + cypher );
      logDebug( "Relationships properties list size : " + data.relUnwindList.size() );
    }

    // Run it always without transactions...
    //
    StatementResult result = data.session.writeTransaction( tx -> tx.run( cypher.toString(), properties ) );
    processSummary( result );

    // Clear the list
    //
    data.relUnwindList.clear();
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
        NeoConnectionUtils.createNodeIndex( log, data.session, Collections.singletonList( nodeLabel ), primaryProperties );
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
    if ( data.relUnwindList != null && data.relUnwindList.size() > 0 ) {
      emptyRelationshipsUnwindList();
    }

    // Allow gc
    //
    data.fromUnwindList = new ArrayList<>();
    data.toUnwindList = new ArrayList<>();
    data.relUnwindList = new ArrayList<>();

    if ( data.outputCount > 0 ) {
      data.transaction.success();
      data.transaction.close();
      data.outputCount = 0;
    }
  }

  /**
   * Update the usagemap.  Add all the labels to the node usage.
   *
   * @param labels
   * @param usage
   */
  protected void updateUsageMap( List<String> labels, GraphUsage usage ) throws KettleValueException {

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

    for ( String label : labels ) {
      if ( StringUtils.isNotEmpty( label ) ) {
        labelSet.add( label );
      }
    }
  }
}
