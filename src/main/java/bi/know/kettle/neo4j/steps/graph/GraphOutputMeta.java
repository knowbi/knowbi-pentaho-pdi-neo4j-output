package bi.know.kettle.neo4j.steps.graph;


import org.neo4j.kettle.core.value.ValueMetaGraph;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "Neo4jGraphOutput",
  name = "Neo4j Graph Output",
  description = "Write to a Neo4j graph using an input field mapping",
  image = "neo4j_graph_output.svg",
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/Neo4j-Graph-Output#description"
)
@InjectionSupported( localizationPrefix = "GraphOutput.Injection.", groups = { "MAPPINGS", } )
public class GraphOutputMeta extends BaseStepMeta implements StepMetaInterface {

  private static final String RETURNING_GRAPH = "returning_graph";
  private static final String RETURN_GRAPH_FIELD = "return_graph_field";
  public static final String CONNECTION = "connection";
  public static final String MODEL = "model";
  public static final String BATCH_SIZE = "batch_size";
  public static final String CREATE_INDEXES = "create_indexes";
  public static final String MAPPINGS = "mappings";
  public static final String MAPPING = "mapping";
  public static final String SOURCE_FIELD = "source_field";
  public static final String TARGET_TYPE = "target_type";
  public static final String TARGET_NAME = "target_name";
  public static final String TARGET_PROPERTY = "target_property";
  public static final String VALIDATE_AGAINST_MODEL = "validate_against_model";

  @Injection( name = CONNECTION )
  private String connectionName;

  @Injection( name = MODEL )
  private String model;

  @Injection( name = BATCH_SIZE )
  private String batchSize;

  @Injection( name = CREATE_INDEXES )
  private boolean creatingIndexes;

  @Injection( name = RETURNING_GRAPH )
  private boolean returningGraph;

  @Injection( name = RETURN_GRAPH_FIELD )
  private String returnGraphField;

  @Injection( name = VALIDATE_AGAINST_MODEL )
  private boolean validatingAgainstModel;

  @InjectionDeep
  private List<FieldModelMapping> fieldModelMappings;


  public GraphOutputMeta() {
    super();
    fieldModelMappings = new ArrayList<>();
    creatingIndexes = true;
  }

  @Override public void setDefault() {

  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new GraphOutput( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new GraphOutputData();
  }

  @Override public String getDialogClassName() {
    return GraphOutputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) {

    if (returningGraph) {

      ValueMetaInterface valueMetaGraph = new ValueMetaGraph( Const.NVL(returnGraphField, "graph") );
      valueMetaGraph.setOrigin( name );
      rowMeta.addValueMeta( valueMetaGraph );

    }
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( CONNECTION, connectionName ) );
    xml.append( XMLHandler.addTagValue( MODEL, model ) );
    xml.append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( CREATE_INDEXES, creatingIndexes ) );
    xml.append( XMLHandler.addTagValue( RETURNING_GRAPH, returningGraph ) );
    xml.append( XMLHandler.addTagValue( RETURN_GRAPH_FIELD, returnGraphField ) );
    xml.append( XMLHandler.addTagValue( VALIDATE_AGAINST_MODEL, validatingAgainstModel ) );

    xml.append( XMLHandler.openTag( MAPPINGS ) );
    for ( FieldModelMapping fieldModelMapping : fieldModelMappings ) {
      xml.append( XMLHandler.openTag( MAPPING ) );
      xml.append( XMLHandler.addTagValue( SOURCE_FIELD, fieldModelMapping.getField() ) );
      xml.append( XMLHandler.addTagValue( TARGET_TYPE, ModelTargetType.getCode( fieldModelMapping.getTargetType() ) ) );
      xml.append( XMLHandler.addTagValue( TARGET_NAME, fieldModelMapping.getTargetName() ) );
      xml.append( XMLHandler.addTagValue( TARGET_PROPERTY, fieldModelMapping.getTargetProperty() ) );
      xml.append( XMLHandler.closeTag( MAPPING ) );
    }
    xml.append( XMLHandler.closeTag( MAPPINGS ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, CONNECTION );
    model = XMLHandler.getTagValue( stepnode, MODEL );
    batchSize = XMLHandler.getTagValue( stepnode, BATCH_SIZE );
    creatingIndexes = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, CREATE_INDEXES ) );
    returningGraph = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, RETURNING_GRAPH ) );
    returnGraphField = XMLHandler.getTagValue( stepnode, RETURN_GRAPH_FIELD );
    validatingAgainstModel = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, VALIDATE_AGAINST_MODEL ) );

    // Parse parameter mappings
    //
    Node mappingsNode = XMLHandler.getSubNode( stepnode, MAPPINGS );
    List<Node> mappingNodes = XMLHandler.getNodes( mappingsNode, MAPPING );
    fieldModelMappings = new ArrayList<>();
    for ( Node mappingNode : mappingNodes ) {
      String field = XMLHandler.getTagValue( mappingNode, SOURCE_FIELD );
      ModelTargetType targetType = ModelTargetType.parseCode( XMLHandler.getTagValue( mappingNode, TARGET_TYPE ) );
      String targetName = XMLHandler.getTagValue( mappingNode, TARGET_NAME );
      String targetProperty = XMLHandler.getTagValue( mappingNode, TARGET_PROPERTY );

      fieldModelMappings.add( new FieldModelMapping( field, targetType, targetName, targetProperty ) );
    }

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transId, stepId, CONNECTION, connectionName );
    rep.saveStepAttribute( transId, stepId, MODEL, model );
    rep.saveStepAttribute( transId, stepId, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transId, stepId, CREATE_INDEXES, creatingIndexes );
    rep.saveStepAttribute( transId, stepId, RETURNING_GRAPH, returningGraph );
    rep.saveStepAttribute( transId, stepId, RETURN_GRAPH_FIELD, returnGraphField );

    for ( int i = 0; i < fieldModelMappings.size(); i++ ) {
      FieldModelMapping fieldModelMapping = fieldModelMappings.get( i );
      rep.saveStepAttribute( transId, stepId, i, SOURCE_FIELD, fieldModelMapping.getField() );
      rep.saveStepAttribute( transId, stepId, i, TARGET_TYPE, ModelTargetType.getCode( fieldModelMapping.getTargetType() ) );
      rep.saveStepAttribute( transId, stepId, i, TARGET_NAME, fieldModelMapping.getField() );
      rep.saveStepAttribute( transId, stepId, i, TARGET_PROPERTY, fieldModelMapping.getField() );
    }

  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( stepId, CONNECTION );
    model = rep.getStepAttributeString( stepId, MODEL );
    batchSize = rep.getStepAttributeString( stepId, BATCH_SIZE );
    creatingIndexes = rep.getStepAttributeBoolean( stepId, CREATE_INDEXES );
    returningGraph = rep.getStepAttributeBoolean( stepId, RETURNING_GRAPH );
    returnGraphField = rep.getStepAttributeString( stepId, RETURN_GRAPH_FIELD );

    fieldModelMappings = new ArrayList<>();
    int nrMappings = rep.countNrStepAttributes( stepId, SOURCE_FIELD );
    for ( int i = 0; i < nrMappings; i++ ) {
      String field = rep.getStepAttributeString( stepId, i, SOURCE_FIELD );
      ModelTargetType targetType = ModelTargetType.parseCode( rep.getStepAttributeString( stepId, i, TARGET_TYPE ) );
      String targetName = rep.getStepAttributeString( stepId, i, TARGET_NAME );
      String targetProperty = rep.getStepAttributeString( stepId, i, TARGET_PROPERTY );

      fieldModelMappings.add( new FieldModelMapping( field, targetType, targetName, targetProperty ) );
    }

  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The connectionName to set
   */
  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }

  /**
   * Gets model
   *
   * @return value of model
   */
  public String getModel() {
    return model;
  }

  /**
   * @param model The model to set
   */
  public void setModel( String model ) {
    this.model = model;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize The batchSize to set
   */
  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  /**
   * Gets creatingIndexes
   *
   * @return value of creatingIndexes
   */
  public boolean isCreatingIndexes() {
    return creatingIndexes;
  }

  /**
   * @param creatingIndexes The creatingIndexes to set
   */
  public void setCreatingIndexes( boolean creatingIndexes ) {
    this.creatingIndexes = creatingIndexes;
  }

  /**
   * Gets fieldModelMappings
   *
   * @return value of fieldModelMappings
   */
  public List<FieldModelMapping> getFieldModelMappings() {
    return fieldModelMappings;
  }

  /**
   * @param fieldModelMappings The fieldModelMappings to set
   */
  public void setFieldModelMappings( List<FieldModelMapping> fieldModelMappings ) {
    this.fieldModelMappings = fieldModelMappings;
  }

  /**
   * Gets returningGraph
   *
   * @return value of returningGraph
   */
  public boolean isReturningGraph() {
    return returningGraph;
  }

  /**
   * @param returningGraph The returningGraph to set
   */
  public void setReturningGraph( boolean returningGraph ) {
    this.returningGraph = returningGraph;
  }

  /**
   * Gets returnGraphField
   *
   * @return value of returnGraphField
   */
  public String getReturnGraphField() {
    return returnGraphField;
  }

  /**
   * @param returnGraphField The returnGraphField to set
   */
  public void setReturnGraphField( String returnGraphField ) {
    this.returnGraphField = returnGraphField;
  }

  /**
   * Gets validatingAgainstModel
   *
   * @return value of validatingAgainstModel
   */
  public boolean isValidatingAgainstModel() {
    return validatingAgainstModel;
  }

  /**
   * @param validatingAgainstModel The validatingAgainstModel to set
   */
  public void setValidatingAgainstModel( boolean validatingAgainstModel ) {
    this.validatingAgainstModel = validatingAgainstModel;
  }
}
