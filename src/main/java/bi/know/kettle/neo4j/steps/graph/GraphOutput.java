package bi.know.kettle.neo4j.steps.graph;


import bi.know.kettle.neo4j.core.GraphUsage;
import bi.know.kettle.neo4j.core.MetaStoreUtil;
import bi.know.kettle.neo4j.model.GraphModel;
import bi.know.kettle.neo4j.model.GraphModelUtils;
import bi.know.kettle.neo4j.model.GraphNode;
import bi.know.kettle.neo4j.model.GraphProperty;
import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.model.GraphRelationship;
import bi.know.kettle.neo4j.shared.DriverSingleton;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import bi.know.kettle.neo4j.steps.BaseNeoStep;
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
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class GraphOutput extends BaseNeoStep implements StepInterface {

  private GraphOutputMeta meta;
  private GraphOutputData data;

  public GraphOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                      TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    meta = (GraphOutputMeta) smi;
    data = (GraphOutputData) sdi;

    // Load some extra metadata...
    //
    if ( StringUtils.isEmpty(meta.getConnectionName()) ) {
      log.logError( "You need to specify a Neo4j connection to use in this step" );
      return false;
    }
    try {
      // To correct lazy programmers who built certain PDI steps...
      //
      data.metaStore = MetaStoreUtil.findMetaStore( this );

      data.neoConnection = NeoConnectionUtils.getConnectionFactory( data.metaStore ).loadElement( meta.getConnectionName() );
      data.neoConnection.initializeVariablesFrom( this );

      if ( StringUtils.isEmpty( meta.getModel() ) ) {
        logError( "No model name is specified" );
        return false;
      }
      data.graphModel = GraphModelUtils.getModelFactory( data.metaStore ).loadElement( meta.getModel() );
      if ( data.graphModel == null ) {
        logError( "Model '" + meta.getModel() + "' could not be found!" );
        return false;
      }
    } catch ( MetaStoreException e ) {
      log.logError( "Could not load connection'" + meta.getConnectionName() + "' from the metastore", e );
      return false;
    }

    data.batchSize = Const.toLong( environmentSubstitute( meta.getBatchSize() ), 1 );

    data.nodeCount = countDistinctNodes( meta.getFieldModelMappings() );

    try {
      data.driver = DriverSingleton.getDriver( log, data.neoConnection );
    } catch ( Exception e ) {
      log.logError( "Unable to get or create Neo4j database driver for database '" + data.neoConnection.getName() + "'", e );
      return false;
    }

    return super.init( smi, sdi );
  }

  private int countDistinctNodes( List<FieldModelMapping> fieldModelMappings ) {
    List<String> nodes = new ArrayList<>();
    for ( FieldModelMapping mapping : fieldModelMappings ) {
      if ( !nodes.contains( mapping.getTargetName() ) ) {
        nodes.add( mapping.getTargetName() );
      }
    }
    return nodes.size();
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    data = (GraphOutputData) sdi;

    wrapUpTransaction();

    if ( data.session != null ) {
      data.session.close();
    }
    if (data.cypherMap!=null) {
      data.cypherMap.clear();
    }

    super.dispose( smi, sdi );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (GraphOutputMeta) smi;
    data = (GraphOutputData) sdi;

    // Only if we actually have previous steps to read from...
    // This way the step also acts as an GraphOutput query step
    //
    Object[] row = getRow();
    if ( row == null ) {
      // Signal next step(s) we're done processing
      //
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, data.metaStore );

      // Get parameter field indexes
      data.fieldIndexes = new int[ meta.getFieldModelMappings().size() ];
      for ( int i = 0; i < meta.getFieldModelMappings().size(); i++ ) {
        String field = meta.getFieldModelMappings().get( i ).getField();
        data.fieldIndexes[ i ] = getInputRowMeta().indexOfValue( field );
        if ( data.fieldIndexes[ i ] < 0 ) {
          throw new KettleException( "Unable to find parameter field '" + field );
        }
      }

      // Create a session
      //
      data.session = data.driver.session();

      // See if we need to create indexes...
      //
      if ( meta.isCreatingIndexes() ) {
        createNodePropertyIndexes( meta, data );
      }

      data.cypherMap = new HashMap<>();
    }

    // Calculate cypher statement, parameters, ... based on field-model-mappings
    //
    Map<String, Object> parameters = new HashMap<>();
    String cypher = getCypher( data.graphModel, meta.getFieldModelMappings(), data.nodeCount, row, getInputRowMeta(), data.fieldIndexes, parameters );
    if ( log.isDebug() ) {
      logDebug( "Parameters found : " + parameters.size() );
      logDebug( "Merge statement : " + cypher );
    }

    boolean errors = executeStatement( data, cypher, parameters );
    if (errors) {
      // Stop processing on error
      //
      setErrors( 1L );
      setOutputDone();
      return false;
    }

    putRow( getInputRowMeta(), row );
    return true;
  }

  private void createNodePropertyIndexes( GraphOutputMeta meta, GraphOutputData data )
    throws KettleException {

    // Only try to create an index on the first step copy
    //
    if ( getCopy() > 0 ) {
      return;
    }

    Map<GraphNode, List<String>> nodePropertiesMap = new HashMap<>();

    for ( int f = 0; f < meta.getFieldModelMappings().size(); f++ ) {
      FieldModelMapping fieldModelMapping = meta.getFieldModelMappings().get( f );

      // We pre-calculated the field indexes
      //
      int index = data.fieldIndexes[ f ];

      // Determine the target property and type
      //
      GraphNode node = data.graphModel.findNode( fieldModelMapping.getTargetName() );
      if ( node == null ) {
        throw new KettleException( "Unable to find target node '" + fieldModelMapping.getTargetName() + "'" );
      }
      GraphProperty graphProperty = node.findProperty( fieldModelMapping.getTargetProperty() );
      if ( graphProperty == null ) {
        throw new KettleException(
          "Unable to find target property '" + fieldModelMapping.getTargetProperty() + "' of node '" + fieldModelMapping.getTargetName() + "'" );
      }

      // See if this is a primary property...
      //
      if ( graphProperty.isPrimary() ) {

        List<String> propertiesList = nodePropertiesMap.get( node );
        if ( propertiesList == null ) {
          propertiesList = new ArrayList<>();
          nodePropertiesMap.put( node, propertiesList );
        }
        propertiesList.add( graphProperty.getName() );
      }
    }

    // Loop over map keys...
    //
    for ( GraphNode node : nodePropertiesMap.keySet() ) {
      NeoConnectionUtils.createNodeIndex( log, data.session, node.getLabels(), nodePropertiesMap.get( node ) );
    }
  }

  private boolean executeStatement( GraphOutputData data, String cypher, Map<String, Object> parameters ) {
    StatementResult result;
    boolean errors = false;
    if ( data.batchSize <= 1 ) {
      result = data.session.run( cypher, parameters );
      errors = processSummary(result);
    } else {
      if ( data.outputCount == 0 ) {
        data.transaction = data.session.beginTransaction();
      }
      result = data.transaction.run( cypher, parameters );
      errors = processSummary(result);

      data.outputCount++;
      incrementLinesOutput();

      if ( !errors && data.outputCount >= data.batchSize ) {
        data.transaction.success();
        data.transaction.close();
        data.outputCount = 0;
      }
    }

    if (errors) {
      setErrors( 1L );
      stopAll();
      setOutputDone();
    }

    if ( log.isDebug() ) {
      logDebug( "Result : " + result.toString() );
    }
    return errors;
  }

  private boolean processSummary( StatementResult result ) {
    boolean errors = false;
    ResultSummary summary = result.consume();
    for ( Notification notification : summary.notifications() ) {
      log.logError( notification.title()+" ("+notification.severity()+")" );
      log.logError(notification.code()+" : "+notification.description()+", position "+notification.position());
      errors=true;
    }
    return errors;
  }

  private class NodeAndPropertyData {
    public GraphNode node;
    public GraphProperty property;
    public ValueMetaInterface sourceValueMeta;
    public Object sourceValueData;
    public int sourceFieldIndex;

    public NodeAndPropertyData( GraphNode node, GraphProperty property, ValueMetaInterface sourceValueMeta, Object sourceValueData, int sourceFieldIndex ) {
      this.node = node;
      this.property = property;
      this.sourceValueMeta = sourceValueMeta;
      this.sourceValueData = sourceValueData;
      this.sourceFieldIndex = sourceFieldIndex;
    }
  }

  /**
   * Generate the Cypher statement and parameters to use to update using a graph model, a field mapping and a row of data
   *
   * @param graphModel         The model to use
   * @param fieldModelMappings The mappings
   * @param nodeCount
   * @param row                The input row
   * @param rowMeta            the input row metadata
   * @param parameters         The parameters map to update
   * @return The generated cypher statement
   */
  protected String getCypher( GraphModel graphModel, List<FieldModelMapping> fieldModelMappings, int nodeCount, Object[] row,
                              RowMetaInterface rowMeta, int[] fieldIndexes, Map<String, Object> parameters ) throws KettleException {


    // We need to cache the Cypher and parameter mappings for performance
    // Basically this is determined by the bitmap of used fields being null or not null
    //
    StringBuffer pattern = new StringBuffer();
    for (int index : data.fieldIndexes) {
      boolean isNull = rowMeta.isNull( row, index );
      pattern.append(isNull?'0':'1');
    }
    CypherParameters cypherParameters = data.cypherMap.get(pattern.toString());
    if (cypherParameters!=null) {
      setParameters(rowMeta, row, parameters, cypherParameters);

      // That's it, return the cypher we previously calculated
      //
      return cypherParameters.getCypher();
    }

    cypherParameters = new CypherParameters();

    // The strategy is to determine all the nodes involved and the properties to set.
    // TODO: Later we'll add relationship properties
    // Then we can determine the relationships between the nodes
    //
    List<GraphNode> nodes = new ArrayList<>();
    List<NodeAndPropertyData> nodeProperties = new ArrayList<>();
    for ( int f = 0; f < fieldModelMappings.size(); f++ ) {
      FieldModelMapping fieldModelMapping = fieldModelMappings.get( f );

      // We pre-calculated the field indexes
      //
      int index = fieldIndexes[ f ];

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( index );
      Object valueData = row[ index ];

      // Determine the target property and type
      //
      GraphNode node = graphModel.findNode( fieldModelMapping.getTargetName() );
      if ( node == null ) {
        throw new KettleException( "Unable to find target node '" + fieldModelMapping.getTargetName() + "'" );
      }
      GraphProperty graphProperty = node.findProperty( fieldModelMapping.getTargetProperty() );
      if ( graphProperty == null ) {
        throw new KettleException(
          "Unable to find target property '" + fieldModelMapping.getTargetProperty() + "' of node '" + fieldModelMapping.getTargetName() + "'" );
      }
      if ( !nodes.contains( node ) ) {
        nodes.add( node );
      }
      nodeProperties.add( new NodeAndPropertyData( node, graphProperty, valueMeta, valueData, index ) );
    }

    // Evaluate wether or not the node property is primary and null
    // In that case, we remove these nodes from the lists...
    //
    Set<GraphNode> ignored = new HashSet<>();
    for ( NodeAndPropertyData nodeProperty : nodeProperties ) {
      if ( nodeProperty.property.isPrimary() ) {
        // Null value?
        //
        if ( nodeProperty.sourceValueMeta.isNull( nodeProperty.sourceValueData ) ) {
          if ( log.isDebug() ) {
            logDebug( "Detected primary null property for node " + nodeProperty.node + " property " + nodeProperty.property + " value : "
              + nodeProperty.sourceValueMeta.getString( nodeProperty.sourceValueData ) );
          }

          if ( !ignored.contains( nodeProperty.node ) ) {
            ignored.add( nodeProperty.node );
          }
        }
      }
    }

    // Now we'll see which relationships are involved between any 2 nodes.
    // Then we can generate the cypher statement as well...
    //
    // v1.0 vanilla algorithm test
    //
    List<GraphRelationship> relationships = new ArrayList<>();
    for ( int x = 0; x < nodes.size(); x++ ) {
      for ( int y = 0; y < nodes.size(); y++ ) {
        if ( x == y ) {
          continue;
        }
        GraphNode sourceNode = nodes.get( x );
        GraphNode targetNode = nodes.get( y );

        GraphRelationship relationship = graphModel.findRelationship( sourceNode.getName(), targetNode.getName() );
        if ( relationship != null ) {
          if ( !relationships.contains( relationship ) ) {
            // A new relationship we don't have yet.
            //
            relationships.add( relationship );
          }
        }
      }
    }

    if ( log.isDebug() ) {
      logDebug( "Found " + relationships.size() + " relationships to consider : " + relationships.toString() );
      logDebug( "Found " + ignored.size() + " nodes to ignore : " + ignored.toString() );
    }

    // Now we have a bunch of Node-Pairs to update...
    //
    int relationshipIndex = 0;
    AtomicInteger parameterIndex = new AtomicInteger( 0 );
    AtomicInteger nodeIndex = new AtomicInteger( 0 );

    StringBuilder cypher = new StringBuilder();

    Set<GraphNode> handled = new HashSet<>();
    Map<GraphNode, Integer> nodeIndexMap = new HashMap<>();

    // No relationships case...
    //
    if ( nodes.size() == 1 ) {
      GraphNode node = nodes.get( 0 );
      addNodeCypher( cypher, node, handled, ignored, parameterIndex, nodeIndex, nodeIndexMap, nodeProperties, parameters, cypherParameters );
    } else {
      for ( GraphRelationship relationship : relationships ) {
        relationshipIndex++;
        if ( log.isDebug() ) {
          logDebug( "Handling relationship : " + relationship.getName() );
        }
        GraphNode nodeSource = graphModel.findNode( relationship.getNodeSource() );
        GraphNode nodeTarget = graphModel.findNode( relationship.getNodeTarget() );

        for ( GraphNode node : new GraphNode[] { nodeSource, nodeTarget } ) {
          addNodeCypher( cypher, node, handled, ignored, parameterIndex, nodeIndex, nodeIndexMap, nodeProperties, parameters, cypherParameters );
        }

        // Now add the merge on the relationship...
        //
        if ( nodeIndexMap.get( nodeSource ) != null && nodeIndexMap.get( nodeTarget ) != null ) {
          String sourceNodeName = "node" + nodeIndexMap.get( nodeSource );
          String targetNodeName = "node" + nodeIndexMap.get( nodeTarget );

          cypher.append( "MERGE(" + sourceNodeName + ")-[rel" + relationshipIndex + ":" + relationship.getLabel() + "]-(" + targetNodeName + ") " );
          cypher.append( Const.CR );

          updateUsageMap( Arrays.asList(relationship.getLabel()), GraphUsage.RELATIONSHIP_UPDATE );
        }
      }
      cypher.append( ";" + Const.CR );
    }

    cypherParameters.setCypher( cypher.toString() );
    data.cypherMap.put(pattern.toString(), cypherParameters);

    return cypher.toString();
  }

  private void setParameters( RowMetaInterface rowMeta, Object[] row, Map<String, Object> parameters, CypherParameters cypherParameters ) throws KettleValueException {
    for (TargetParameter targetParameter : cypherParameters.getTargetParameters()) {
      int fieldIndex = targetParameter.getInputFieldIndex();
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( fieldIndex );
      Object valueData = row[fieldIndex];
      String parameterName = targetParameter.getParameterName();
      GraphPropertyType parameterType = targetParameter.getParameterType();

      // Convert to the neo type
      //
      Object neoObject = parameterType.convertFromKettle( valueMeta, valueData );

      parameters.put(parameterName, neoObject);
    }
  }

  private void addNodeCypher( StringBuilder cypher, GraphNode node,
                              Set<GraphNode> handled, Set<GraphNode> ignored,
                              AtomicInteger parameterIndex, AtomicInteger nodeIndex,
                              Map<GraphNode, Integer> nodeIndexMap,
                              List<NodeAndPropertyData> nodeProperties, Map<String, Object> parameters,
                              CypherParameters cypherParameters) throws KettleValueException {
    if ( !ignored.contains( node ) && !handled.contains( node ) ) {

      // Don't update twice.
      //
      handled.add( node );
      nodeIndexMap.put( node, nodeIndex.incrementAndGet() );

      // Calculate the node labels
      //
      String nodeLabels = "";
      for ( String nodeLabel : node.getLabels() ) {
        nodeLabels += ":";
        nodeLabels += nodeLabel;
      }

      String matchCypher = "";

      String nodeAlias = "node" + nodeIndex;

      cypher.append( "MERGE (" + nodeAlias + nodeLabels + " { " );

      updateUsageMap( node.getLabels(), GraphUsage. NODE_UPDATE );

      if ( log.isDebug() ) {
        logBasic( " - node merge : " + node.getName() );
      }

      // Look up the properties to update in the node
      //
      boolean firstPrimary = true;
      boolean firstMatch = true;
      for ( NodeAndPropertyData napd : nodeProperties ) {
        if ( napd.node.equals( node ) ) {
          // Handle the property
          //
          parameterIndex.incrementAndGet();
          boolean isNull = napd.sourceValueMeta.isNull( napd.sourceValueData );
          String parameterName = "param" + parameterIndex;

          if ( napd.property.isPrimary() ) {

            if ( !firstPrimary ) {
              cypher.append( ", " );
            }
            cypher.append( napd.property.getName() + " : {" + parameterName + "} " );

            firstPrimary = false;

            if ( log.isDebug() ) {
              logBasic( "   * property match/create : " + napd.property.getName() + " with value " + napd.sourceValueMeta.toStringMeta() + " : "
                + napd.sourceValueMeta.getString( napd.sourceValueData ) );
            }

          } else {
            // On match statement
            //
            if ( firstMatch ) {
              matchCypher += "SET ";
            } else {
              matchCypher += ", ";
            }

            firstMatch = false;

            matchCypher += nodeAlias + "." + napd.property.getName() + " = ";
            if ( isNull ) {
              matchCypher += "NULL ";
            } else {
              matchCypher += "{" + parameterName + "} ";
            }

            if ( log.isDebug() ) {
              logBasic( "   * property update : " + napd.property.getName() + " with value " + napd.sourceValueMeta.toStringMeta() + " : "
                + napd.sourceValueMeta.getString( napd.sourceValueData ) );
            }
          }

          // NULL parameters are better set with NULL directly
          //
          if ( !isNull ) {
            parameters.put( parameterName, napd.property.getType().convertFromKettle( napd.sourceValueMeta, napd.sourceValueData ) );
            TargetParameter targetParameter = new TargetParameter( napd.sourceValueMeta.getName(), napd.sourceFieldIndex, parameterName, napd.property.getType() );
            cypherParameters.getTargetParameters().add( targetParameter );
          }
        }
      }
      cypher.append( "}) " + Const.CR );

      // Add an OM MATCH SET clause if there are any non-primary key fields to update
      //
      if ( StringUtils.isNotEmpty( matchCypher ) ) {
        cypher.append( matchCypher );
      }
    }
  }

  @Override public void batchComplete() {
   wrapUpTransaction();
  }

  private void wrapUpTransaction() {
    if ( data.outputCount > 0 ) {
      data.transaction.success();
      data.transaction.close();

      // Force creation of a new transaction on the next batch of records
      //
      data.outputCount=0;
    }
  }

  /**
   * Update the usagemap.  Add all the labels to the node usage.
   *  @param nodeLabels
   * @param usage
   */
  protected void updateUsageMap( List<String> nodeLabels, GraphUsage usage ) throws KettleValueException {
    Map<String, Set<String>> stepsMap = data.usageMap.get( usage.name() );
    if (stepsMap==null) {
      stepsMap = new HashMap<>();
      data.usageMap.put(usage.name(), stepsMap );
    }

    Set<String> labelSet = stepsMap.get( getStepname() );
    if (labelSet==null) {
      labelSet = new HashSet<>(  );
      stepsMap.put(getStepname(), labelSet);
    }

    for ( String label: nodeLabels) {
      if ( StringUtils.isNotEmpty( label ) ) {
        labelSet.add( label );
      }
    }
  }
}
