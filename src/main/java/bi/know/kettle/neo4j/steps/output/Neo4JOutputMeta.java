package bi.know.kettle.neo4j.steps.output;

import bi.know.kettle.neo4j.core.value.ValueMetaGraph;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
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
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;


@Step(
  id = "Neo4JOutput",
  image = "neo4j_output.svg",
  i18nPackageName = "bi.know.kettle.neo4j.steps.output",
  name = "Neo4JOutput.Step.Name",
  description = "Neo4JOutput.Step.Description",
  categoryDescription = "Neo4JOutput.Step.Category",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/Neo4j-Output#description"
)
@InjectionSupported( localizationPrefix = "Neo4JOutput.Injection.",
  groups = { "FROM_NODE_PROPS", "FROM_LABELS", "TO_NODE_PROPS", "TO_LABELS", "REL_PROPS" } )
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface {

  private static final String STRING_CONNECTION = "connection";
  private static final String STRING_BATCH_SIZE = "batch_size";
  private static final String STRING_KEY = "key";
  private static final String STRING_FROM = "from";
  private static final String STRING_LABELS = "labels";
  private static final String STRING_LABEL = "label";
  private static final String STRING_VALUE = "value";
  private static final String STRING_PROPERTIES = "properties";
  private static final String STRING_PROPERTY = "property";
  private static final String STRING_PROPERTY_NAME = "name";
  private static final String STRING_PROPERTY_VALUE = "value";
  private static final String STRING_PROPERTY_TYPE = "type";
  private static final String STRING_PROPERTY_PRIMARY = "primary";
  private static final String STRING_TO = "to";
  private static final String STRING_RELATIONSHIP = "relationship";
  private static final String STRING_RELATIONSHIP_VALUE = "relationship_value";
  private static final String STRING_RELPROPS = "relprops";
  private static final String STRING_RELPROP = "relprop";
  private static final String STRING_CREATE_INDEXES = "create_indexes";
  private static final String STRING_USE_CREATE = "use_create";
  private static final String STRING_ONLY_CREATE_RELATIONSHIPS = "only_create_relationships";
  private static final String STRING_READ_ONLY_FROM_NODE= "read_only_from_node";
  private static final String STRING_READ_ONLY_TO_NODE= "read_only_to_node";

  private static final String STRING_RETURNING_GRAPH = "returning_graph";
  private static final String STRING_RETURN_GRAPH_FIELD = "return_graph_field";


  @Injection( name = STRING_CONNECTION )
  private String connection;
  @Injection( name = STRING_BATCH_SIZE )
  private String batchSize;
  @Injection( name = STRING_CREATE_INDEXES )
  private boolean creatingIndexes;
  @Injection( name = STRING_USE_CREATE )
  private boolean usingCreate;
  @Injection( name = STRING_ONLY_CREATE_RELATIONSHIPS )
  private boolean onlyCreatingRelationships;
  @Injection( name = STRING_READ_ONLY_FROM_NODE )
  private boolean readOnlyFromNode;
  @Injection( name = STRING_READ_ONLY_TO_NODE )
  private boolean readOnlyToNode;

  @Injection( name = "FROM_NODE_PROPERTY_FIELD", group = "FROM_NODE_PROPS" )
  private String[] fromNodeProps;
  @Injection( name = "FROM_NODE_PROPERTY_NAME", group = "FROM_NODE_PROPS" )
  private String[] fromNodePropNames;
  @Injection( name = "FROM_NODE_PROPERTY_TYPE", group = "FROM_NODE_PROPS" )
  private String[] fromNodePropTypes;
  @Injection( name = "FROM_NODE_PROPERTY_PRIMARY", group = "FROM_NODE_PROPS" )
  private boolean[] fromNodePropPrimary;

  @Injection( name = "TO_NODE_PROPERTY_FIELD", group = "TO_NODE_PROPS" )
  private String[] toNodeProps;
  @Injection( name = "TO_NODE_PROPERTY_NAME", group = "TO_NODE_PROPS" )
  private String[] toNodePropNames;
  @Injection( name = "TO_NODE_PROPERTY_TYPE", group = "TO_NODE_PROPS" )
  private String[] toNodePropTypes;
  @Injection( name = "TO_NODE_PROPERTY_PRIMARY", group = "TO_NODE_PROPS" )
  private boolean[] toNodePropPrimary;

  @Injection( name = "FROM_LABEL", group = "FROM_LABELS" )
  private String[] fromNodeLabels;
  @Injection( name = "TO_LABEL", group = "TO_LABELS" )
  private String[] toNodeLabels;

  @Injection( name = "FROM_LABEL_VALUE", group = "FROM_LABELS" )
  private String[] fromNodeLabelValues;
  @Injection( name = "TO_LABEL_VALUE", group = "TO_LABELS" )
  private String[] toNodeLabelValues;

  @Injection( name = "REL_PROPERTY_FIELD", group = "REL_PROPS" )
  private String[] relProps;
  @Injection( name = "REL_PROPERTY_NAME", group = "REL_PROPS" )
  private String[] relPropNames;
  @Injection( name = "REL_PROPERTY_TYPE", group = "REL_PROPS" )
  private String[] relPropTypes;

  @Injection( name = "REL_LABEL" )
  private String relationship;

  @Injection( name = "REL_VALUE" )
  private String relationshipValue;

  @Injection( name = STRING_RETURNING_GRAPH )
  private boolean returningGraph;

  @Injection( name = STRING_RETURN_GRAPH_FIELD )
  private String returnGraphField;


  // Unused fields from previous version
  //
  private String key;

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp ) {
    return new Neo4JOutput( stepMeta, stepDataInterface, cnr, transMeta, disp );
  }

  public StepDataInterface getStepData() {
    return new Neo4JOutputData();
  }

  public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
    return new Neo4JOutputDialog( shell, meta, transMeta, name );
  }

  public void setDefault() {
    connection = "";
    batchSize = "1000";
    creatingIndexes = true;
    usingCreate = false;
    onlyCreatingRelationships = false;

    fromNodeLabels = new String[ 0 ];
    fromNodeLabelValues = new String[ 0 ];
    fromNodeProps = new String[ 0 ];
    fromNodePropNames = new String[ 0 ];
    fromNodePropTypes = new String[ 0 ];
    fromNodePropPrimary = new boolean[ 0 ];

    toNodeLabels = new String[ 0 ];
    toNodeLabelValues = new String[ 0 ];
    toNodeProps = new String[ 0 ];
    toNodePropNames = new String[ 0 ];
    toNodePropTypes = new String[ 0 ];
    toNodePropPrimary = new boolean[ 0 ];

    relProps = new String[ 0 ];
    relPropNames = new String[ 0 ];
    relPropTypes = new String[ 0 ];

    returnGraphField = "graph";
  }

  public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();

    xml.append( XMLHandler.addTagValue( STRING_CONNECTION, connection ) );
    xml.append( XMLHandler.addTagValue( STRING_BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( STRING_KEY, key ) );
    xml.append( XMLHandler.addTagValue( STRING_CREATE_INDEXES, creatingIndexes ) );
    xml.append( XMLHandler.addTagValue( STRING_USE_CREATE, usingCreate ) );
    xml.append( XMLHandler.addTagValue( STRING_ONLY_CREATE_RELATIONSHIPS, onlyCreatingRelationships) );
    xml.append( XMLHandler.addTagValue( STRING_RETURNING_GRAPH, returningGraph ) );
    xml.append( XMLHandler.addTagValue( STRING_RETURN_GRAPH_FIELD, returnGraphField) );

    xml.append( XMLHandler.openTag( STRING_FROM ) );

    xml.append( XMLHandler.addTagValue( STRING_READ_ONLY_FROM_NODE, readOnlyFromNode) );

    xml.append( XMLHandler.openTag( STRING_LABELS ) );
    for ( int i = 0; i < fromNodeLabels.length; i++ ) {
      xml.append( XMLHandler.addTagValue( STRING_LABEL, fromNodeLabels[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_VALUE, fromNodeLabelValues[ i ] ) );
    }
    xml.append( XMLHandler.closeTag( STRING_LABELS ) );

    xml.append( XMLHandler.openTag( STRING_PROPERTIES ) );
    for ( int i = 0; i < fromNodeProps.length; i++ ) {
      xml.append( XMLHandler.openTag( STRING_PROPERTY ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_NAME, fromNodePropNames[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_VALUE, fromNodeProps[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_TYPE, fromNodePropTypes[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_PRIMARY, fromNodePropPrimary[ i ] ) );
      xml.append( XMLHandler.closeTag( STRING_PROPERTY ) );
    }
    xml.append( XMLHandler.closeTag( STRING_PROPERTIES ) );
    xml.append( XMLHandler.closeTag( STRING_FROM ) );

    xml.append( XMLHandler.openTag( STRING_TO ) );

    xml.append( XMLHandler.addTagValue( STRING_READ_ONLY_TO_NODE, readOnlyToNode) );

    xml.append( XMLHandler.openTag( STRING_LABELS ) );
    for ( int i = 0; i < toNodeLabels.length; i++ ) {
      xml.append( XMLHandler.addTagValue( STRING_LABEL, toNodeLabels[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_VALUE, toNodeLabelValues[ i ] ) );
    }
    xml.append( XMLHandler.closeTag( STRING_LABELS ) );

    xml.append( XMLHandler.openTag( STRING_PROPERTIES ) );
    for ( int i = 0; i < toNodeProps.length; i++ ) {
      xml.append( XMLHandler.openTag( STRING_PROPERTY ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_NAME, toNodePropNames[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_VALUE, toNodeProps[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_TYPE, toNodePropTypes[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_PRIMARY, toNodePropPrimary[ i ] ) );
      xml.append( XMLHandler.closeTag( STRING_PROPERTY ) );
    }
    xml.append( XMLHandler.closeTag( STRING_PROPERTIES ) );
    xml.append( XMLHandler.closeTag( STRING_TO ) );

    xml.append( XMLHandler.addTagValue( STRING_RELATIONSHIP, relationship ) );
    xml.append( XMLHandler.addTagValue( STRING_RELATIONSHIP_VALUE, relationshipValue ) );

    xml.append( XMLHandler.openTag( STRING_RELPROPS ) );
    for ( int i = 0; i < relProps.length; i++ ) {
      xml.append( XMLHandler.openTag( STRING_RELPROP ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_NAME, relPropNames[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_VALUE, relProps[ i ] ) );
      xml.append( XMLHandler.addTagValue( STRING_PROPERTY_TYPE, relPropTypes[ i ] ) );
      xml.append( XMLHandler.closeTag( STRING_RELPROP ) );
    }
    xml.append( XMLHandler.closeTag( STRING_RELPROPS ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    connection = XMLHandler.getTagValue( stepnode, STRING_CONNECTION );
    batchSize = XMLHandler.getTagValue( stepnode, STRING_BATCH_SIZE );
    key = XMLHandler.getTagValue( stepnode, STRING_KEY );
    creatingIndexes = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_CREATE_INDEXES ) );
    usingCreate = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_USE_CREATE ) );
    onlyCreatingRelationships = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_ONLY_CREATE_RELATIONSHIPS) );
    returningGraph = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_RETURNING_GRAPH ) );
    returnGraphField = XMLHandler.getTagValue( stepnode, STRING_RETURN_GRAPH_FIELD );

    Node fromNode = XMLHandler.getSubNode( stepnode, STRING_FROM );

    readOnlyFromNode = "Y".equalsIgnoreCase( XMLHandler.getTagValue( fromNode, STRING_READ_ONLY_FROM_NODE ) );

    Node fromLabelsNode = XMLHandler.getSubNode( fromNode, STRING_LABELS );
    List<Node> fromLabelNodes = XMLHandler.getNodes( fromLabelsNode, STRING_LABEL );
    List<Node> fromLabelValueNodes = XMLHandler.getNodes( fromLabelsNode, STRING_VALUE );

    fromNodeLabels = new String[ fromLabelNodes.size() ];
    fromNodeLabelValues = new String[ Math.max(fromLabelValueNodes.size(), fromLabelNodes.size()) ];

    for ( int i = 0; i < fromLabelNodes.size(); i++ ) {
      Node labelNode = fromLabelNodes.get( i );
      fromNodeLabels[ i ] = XMLHandler.getNodeValue( labelNode );
    }
    for ( int i = 0; i < fromLabelValueNodes.size(); i++ ) {
      Node valueNode = fromLabelValueNodes.get( i );
      fromNodeLabelValues[ i ] = XMLHandler.getNodeValue( valueNode );
    }

    Node fromPropsNode = XMLHandler.getSubNode( fromNode, STRING_PROPERTIES );
    List<Node> fromPropertyNodes = XMLHandler.getNodes( fromPropsNode, STRING_PROPERTY );

    fromNodeProps = new String[ fromPropertyNodes.size() ];
    fromNodePropNames = new String[ fromPropertyNodes.size() ];
    fromNodePropTypes = new String[ fromPropertyNodes.size() ];
    fromNodePropPrimary = new boolean[ fromPropertyNodes.size() ];

    for ( int i = 0; i < fromPropertyNodes.size(); i++ ) {
      Node propNode = fromPropertyNodes.get( i );
      fromNodeProps[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_VALUE );
      fromNodePropNames[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_NAME );
      fromNodePropTypes[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_TYPE );
      fromNodePropPrimary[ i ] = "Y".equalsIgnoreCase( XMLHandler.getTagValue( propNode, STRING_PROPERTY_PRIMARY ) );
    }

    Node toNode = XMLHandler.getSubNode( stepnode, STRING_TO );

    readOnlyToNode = "Y".equalsIgnoreCase( XMLHandler.getTagValue( toNode, STRING_READ_ONLY_TO_NODE ) );

    Node toLabelsNode = XMLHandler.getSubNode( toNode, STRING_LABELS );
    List<Node> toLabelNodes = XMLHandler.getNodes( toLabelsNode, STRING_LABEL );
    List<Node> toLabelValueNodes = XMLHandler.getNodes( toLabelsNode, STRING_VALUE );

    toNodeLabels = new String[ toLabelNodes.size() ];
    toNodeLabelValues = new String[ Math.max(toLabelValueNodes.size(), toLabelNodes.size()) ];

    for ( int i = 0; i < toLabelNodes.size(); i++ ) {
      Node labelNode = toLabelNodes.get( i );
      toNodeLabels[ i ] = XMLHandler.getNodeValue( labelNode );
    }
    for ( int i = 0; i < toLabelValueNodes.size(); i++ ) {
      Node valueNode = toLabelValueNodes.get( i );
      toNodeLabelValues[ i ] = XMLHandler.getNodeValue( valueNode );
    }

    Node toPropsNode = XMLHandler.getSubNode( toNode, STRING_PROPERTIES );
    List<Node> toPropertyNodes = XMLHandler.getNodes( toPropsNode, STRING_PROPERTY );

    toNodeProps = new String[ toPropertyNodes.size() ];
    toNodePropNames = new String[ toPropertyNodes.size() ];
    toNodePropTypes = new String[ toPropertyNodes.size() ];
    toNodePropPrimary = new boolean[ toPropertyNodes.size() ];

    for ( int i = 0; i < toPropertyNodes.size(); i++ ) {
      Node propNode = toPropertyNodes.get( i );

      toNodeProps[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_VALUE );
      toNodePropNames[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_NAME );
      toNodePropTypes[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_TYPE );
      toNodePropPrimary[ i ] = "Y".equalsIgnoreCase( XMLHandler.getTagValue( propNode, STRING_PROPERTY_PRIMARY ) );
    }

    relationship = XMLHandler.getTagValue( stepnode, STRING_RELATIONSHIP );
    relationshipValue = XMLHandler.getTagValue( stepnode, STRING_RELATIONSHIP_VALUE );

    Node relPropsNode = XMLHandler.getSubNode( stepnode, STRING_RELPROPS );
    List<Node> relPropNodes = XMLHandler.getNodes( relPropsNode, STRING_RELPROP );

    relProps = new String[ relPropNodes.size() ];
    relPropNames = new String[ relPropNodes.size() ];
    relPropTypes = new String[ relPropNodes.size() ];

    for ( int i = 0; i < relPropNodes.size(); i++ ) {
      Node propNode = relPropNodes.get( i );

      relProps[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_VALUE );
      relPropNames[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_NAME );
      relPropTypes[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_TYPE );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId ) throws KettleException {

    rep.saveStepAttribute( transId, stepId, STRING_CONNECTION, connection );
    rep.saveStepAttribute( transId, stepId, STRING_BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transId, stepId, STRING_KEY, key );
    rep.saveStepAttribute( transId, stepId, STRING_CREATE_INDEXES, creatingIndexes );
    rep.saveStepAttribute( transId, stepId, STRING_USE_CREATE, usingCreate );
    rep.saveStepAttribute( transId, stepId, STRING_ONLY_CREATE_RELATIONSHIPS, onlyCreatingRelationships);
    rep.saveStepAttribute( transId, stepId, STRING_RETURNING_GRAPH, returningGraph );
    rep.saveStepAttribute( transId, stepId, STRING_RETURN_GRAPH_FIELD, returnGraphField );

    rep.saveStepAttribute( transId, stepId, STRING_READ_ONLY_FROM_NODE, readOnlyFromNode);
    rep.saveStepAttribute( transId, stepId, STRING_READ_ONLY_TO_NODE, readOnlyToNode);

    String fromLabelKey = STRING_FROM + "_" + STRING_LABEL;
    for ( int i = 0; i < fromNodeLabels.length; i++ ) {
      rep.saveStepAttribute( transId, stepId, i, fromLabelKey, fromNodeLabels[ i ] );
    }

    String fromPrefix = STRING_FROM + "_" + STRING_PROPERTY + "_";
    for ( int i = 0; i < fromNodeProps.length; i++ ) {

      rep.saveStepAttribute( transId, stepId, i, fromPrefix + STRING_PROPERTY_NAME, fromNodePropNames[ i ] );
      rep.saveStepAttribute( transId, stepId, i, fromPrefix + STRING_PROPERTY_VALUE, fromNodeProps[ i ] );
      rep.saveStepAttribute( transId, stepId, i, fromPrefix + STRING_PROPERTY_TYPE, fromNodePropTypes[ i ] );
      rep.saveStepAttribute( transId, stepId, i, fromPrefix + STRING_PROPERTY_PRIMARY, fromNodePropPrimary[ i ] );
    }

    String toLabelKey = STRING_TO + "_" + STRING_LABEL;
    for ( int i = 0; i < toNodeLabels.length; i++ ) {
      rep.saveStepAttribute( transId, stepId, i, toLabelKey, toNodeLabels[ i ] );
    }

    String toPrefix = STRING_TO + "_" + STRING_PROPERTY + "_";
    for ( int i = 0; i < toNodeProps.length; i++ ) {

      rep.saveStepAttribute( transId, stepId, i, toPrefix + STRING_PROPERTY_NAME, toNodePropNames[ i ] );
      rep.saveStepAttribute( transId, stepId, i, toPrefix + STRING_PROPERTY_VALUE, toNodeProps[ i ] );
      rep.saveStepAttribute( transId, stepId, i, toPrefix + STRING_PROPERTY_TYPE, toNodePropTypes[ i ] );
      rep.saveStepAttribute( transId, stepId, i, toPrefix + STRING_PROPERTY_PRIMARY, toNodePropPrimary[ i ] );
    }

    rep.saveStepAttribute( transId, stepId, STRING_RELATIONSHIP, relationship );
    rep.saveStepAttribute( transId, stepId, STRING_RELATIONSHIP_VALUE, relationshipValue );

    String relPrefix = STRING_RELPROP + "_" + STRING_PROPERTY + "_";
    for ( int i = 0; i < relProps.length; i++ ) {

      rep.saveStepAttribute( transId, stepId, i, relPrefix + STRING_PROPERTY_NAME, relPropNames[ i ] );
      rep.saveStepAttribute( transId, stepId, i, relPrefix + STRING_PROPERTY_VALUE, relProps[ i ] );
      rep.saveStepAttribute( transId, stepId, i, relPrefix + STRING_PROPERTY_TYPE, relPropTypes[ i ] );
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connection = rep.getStepAttributeString( stepId, STRING_CONNECTION );
    batchSize = rep.getStepAttributeString( stepId, STRING_BATCH_SIZE );
    key = rep.getStepAttributeString( stepId, STRING_KEY );
    creatingIndexes = rep.getStepAttributeBoolean( stepId, STRING_CREATE_INDEXES );
    usingCreate = rep.getStepAttributeBoolean( stepId, STRING_USE_CREATE );
    onlyCreatingRelationships = rep.getStepAttributeBoolean( stepId, STRING_ONLY_CREATE_RELATIONSHIPS );
    returningGraph = rep.getStepAttributeBoolean( stepId, STRING_RETURNING_GRAPH );
    returnGraphField = rep.getStepAttributeString( stepId, STRING_RETURN_GRAPH_FIELD );

    readOnlyFromNode = rep.getStepAttributeBoolean( stepId, STRING_READ_ONLY_FROM_NODE);
    readOnlyToNode = rep.getStepAttributeBoolean( stepId, STRING_READ_ONLY_TO_NODE );

    String fromLabelKey = STRING_FROM + "_" + STRING_LABEL;
    int nrFromLabels = rep.countNrStepAttributes( stepId, fromLabelKey );

    fromNodeLabels = new String[ nrFromLabels ];

    for ( int i = 0; i < nrFromLabels; i++ ) {
      fromNodeLabels[ i ] = rep.getStepAttributeString( stepId, i, fromLabelKey );
    }

    String fromPrefix = STRING_FROM + "_" + STRING_PROPERTY + "_";
    int nrFromProps = rep.countNrStepAttributes( stepId, fromPrefix + STRING_PROPERTY_VALUE );

    fromNodeProps = new String[ nrFromProps ];
    fromNodePropNames = new String[ nrFromProps ];
    fromNodePropTypes = new String[ nrFromProps ];
    fromNodePropPrimary = new boolean[ nrFromProps ];

    for ( int i = 0; i < nrFromProps; i++ ) {
      fromNodeProps[ i ] = rep.getStepAttributeString( stepId, i, fromPrefix + STRING_PROPERTY_VALUE );
      fromNodePropNames[ i ] = rep.getStepAttributeString( stepId, i, fromPrefix + STRING_PROPERTY_NAME );
      fromNodePropTypes[ i ] = rep.getStepAttributeString( stepId, i, fromPrefix + STRING_PROPERTY_TYPE );
      fromNodePropPrimary[ i ] = rep.getStepAttributeBoolean( stepId, i, fromPrefix + STRING_PROPERTY_PRIMARY );
    }

    String toLabelKey = STRING_TO + "_" + STRING_LABEL;
    int nrToLabels = rep.countNrStepAttributes( stepId, toLabelKey );

    toNodeLabels = new String[ nrToLabels ];

    for ( int i = 0; i < nrToLabels; i++ ) {
      toNodeLabels[ i ] = rep.getStepAttributeString( stepId, i, toLabelKey );
    }

    String toPrefix = STRING_TO + "_" + STRING_PROPERTY + "_";
    int nrToProps = rep.countNrStepAttributes( stepId, toPrefix + STRING_PROPERTY_VALUE );

    toNodeProps = new String[ nrToProps ];
    toNodePropNames = new String[ nrToProps ];
    toNodePropTypes = new String[ nrToProps ];
    toNodePropPrimary = new boolean[ nrToProps ];

    for ( int i = 0; i < nrToProps; i++ ) {
      toNodeProps[ i ] = rep.getStepAttributeString( stepId, i, toPrefix + STRING_PROPERTY_VALUE );
      toNodePropNames[ i ] = rep.getStepAttributeString( stepId, i, toPrefix + STRING_PROPERTY_NAME );
      toNodePropTypes[ i ] = rep.getStepAttributeString( stepId, i, toPrefix + STRING_PROPERTY_TYPE );
      toNodePropPrimary[ i ] = rep.getStepAttributeBoolean( stepId, i, toPrefix + STRING_PROPERTY_PRIMARY );
    }

    relationship = rep.getStepAttributeString( stepId, STRING_RELATIONSHIP );
    relationshipValue = rep.getStepAttributeString( stepId, STRING_RELATIONSHIP_VALUE );

    String relPrefix = STRING_RELPROP + "_" + STRING_PROPERTY + "_";
    int nrRelProps = rep.countNrStepAttributes( stepId, relPrefix + STRING_PROPERTY_VALUE );

    relProps = new String[ nrRelProps ];
    relPropNames = new String[ nrRelProps ];
    relPropTypes = new String[ nrRelProps ];

    for ( int i = 0; i < nrRelProps; i++ ) {
      relProps[ i ] = rep.getStepAttributeString( stepId, i, relPrefix + STRING_PROPERTY_VALUE );
      relPropNames[ i ] = rep.getStepAttributeString( stepId, i, relPrefix + STRING_PROPERTY_NAME );
      relPropTypes[ i ] = rep.getStepAttributeString( stepId, i, relPrefix + STRING_PROPERTY_TYPE );
    }
  }


  @Override public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input,
                               String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta );
      remarks.add( cr );
    }

    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {
    if (returningGraph) {

      ValueMetaInterface valueMetaGraph = new ValueMetaGraph( Const.NVL(returnGraphField, "graph") );
      valueMetaGraph.setOrigin( name );
      inputRowMeta.addValueMeta( valueMetaGraph );

    }
  }

  public Object clone() {
    return super.clone();
  }

  /**
   * Gets connection
   *
   * @return value of connection
   */
  public String getConnection() {
    return connection;
  }

  /**
   * @param connection The connection to set
   */
  public void setConnection( String connection ) {
    this.connection = connection;
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
   * Gets usingCreate
   *
   * @return value of usingCreate
   */
  public boolean isUsingCreate() {
    return usingCreate;
  }

  /**
   * @param usingCreate The usingCreate to set
   */
  public void setUsingCreate( boolean usingCreate ) {
    this.usingCreate = usingCreate;
  }

  /**
   * Gets onlyCreatingRelationships
   *
   * @return value of onlyCreatingRelationships
   */
  public boolean isOnlyCreatingRelationships() {
    return onlyCreatingRelationships;
  }

  /**
   * @param onlyCreatingRelationships The onlyCreatingRelationships to set
   */
  public void setOnlyCreatingRelationships( boolean onlyCreatingRelationships ) {
    this.onlyCreatingRelationships = onlyCreatingRelationships;
  }

  /**
   * Gets fromNodeProps
   *
   * @return value of fromNodeProps
   */
  public String[] getFromNodeProps() {
    return fromNodeProps;
  }

  /**
   * @param fromNodeProps The fromNodeProps to set
   */
  public void setFromNodeProps( String[] fromNodeProps ) {
    this.fromNodeProps = fromNodeProps;
  }

  /**
   * Gets fromNodePropNames
   *
   * @return value of fromNodePropNames
   */
  public String[] getFromNodePropNames() {
    return fromNodePropNames;
  }

  /**
   * @param fromNodePropNames The fromNodePropNames to set
   */
  public void setFromNodePropNames( String[] fromNodePropNames ) {
    this.fromNodePropNames = fromNodePropNames;
  }

  /**
   * Gets fromNodePropTypes
   *
   * @return value of fromNodePropTypes
   */
  public String[] getFromNodePropTypes() {
    return fromNodePropTypes;
  }

  /**
   * @param fromNodePropTypes The fromNodePropTypes to set
   */
  public void setFromNodePropTypes( String[] fromNodePropTypes ) {
    this.fromNodePropTypes = fromNodePropTypes;
  }

  /**
   * Gets fromNodePropPrimary
   *
   * @return value of fromNodePropPrimary
   */
  public boolean[] getFromNodePropPrimary() {
    return fromNodePropPrimary;
  }

  /**
   * @param fromNodePropPrimary The fromNodePropPrimary to set
   */
  public void setFromNodePropPrimary( boolean[] fromNodePropPrimary ) {
    this.fromNodePropPrimary = fromNodePropPrimary;
  }

  /**
   * Gets toNodeProps
   *
   * @return value of toNodeProps
   */
  public String[] getToNodeProps() {
    return toNodeProps;
  }

  /**
   * @param toNodeProps The toNodeProps to set
   */
  public void setToNodeProps( String[] toNodeProps ) {
    this.toNodeProps = toNodeProps;
  }

  /**
   * Gets toNodePropNames
   *
   * @return value of toNodePropNames
   */
  public String[] getToNodePropNames() {
    return toNodePropNames;
  }

  /**
   * @param toNodePropNames The toNodePropNames to set
   */
  public void setToNodePropNames( String[] toNodePropNames ) {
    this.toNodePropNames = toNodePropNames;
  }

  /**
   * Gets toNodePropTypes
   *
   * @return value of toNodePropTypes
   */
  public String[] getToNodePropTypes() {
    return toNodePropTypes;
  }

  /**
   * @param toNodePropTypes The toNodePropTypes to set
   */
  public void setToNodePropTypes( String[] toNodePropTypes ) {
    this.toNodePropTypes = toNodePropTypes;
  }

  /**
   * Gets toNodePropPrimary
   *
   * @return value of toNodePropPrimary
   */
  public boolean[] getToNodePropPrimary() {
    return toNodePropPrimary;
  }

  /**
   * @param toNodePropPrimary The toNodePropPrimary to set
   */
  public void setToNodePropPrimary( boolean[] toNodePropPrimary ) {
    this.toNodePropPrimary = toNodePropPrimary;
  }

  /**
   * Gets fromNodeLabels
   *
   * @return value of fromNodeLabels
   */
  public String[] getFromNodeLabels() {
    return fromNodeLabels;
  }

  /**
   * @param fromNodeLabels The fromNodeLabels to set
   */
  public void setFromNodeLabels( String[] fromNodeLabels ) {
    this.fromNodeLabels = fromNodeLabels;
  }

  /**
   * Gets toNodeLabels
   *
   * @return value of toNodeLabels
   */
  public String[] getToNodeLabels() {
    return toNodeLabels;
  }

  /**
   * @param toNodeLabels The toNodeLabels to set
   */
  public void setToNodeLabels( String[] toNodeLabels ) {
    this.toNodeLabels = toNodeLabels;
  }

  /**
   * Gets relProps
   *
   * @return value of relProps
   */
  public String[] getRelProps() {
    return relProps;
  }

  /**
   * @param relProps The relProps to set
   */
  public void setRelProps( String[] relProps ) {
    this.relProps = relProps;
  }

  /**
   * Gets relPropNames
   *
   * @return value of relPropNames
   */
  public String[] getRelPropNames() {
    return relPropNames;
  }

  /**
   * @param relPropNames The relPropNames to set
   */
  public void setRelPropNames( String[] relPropNames ) {
    this.relPropNames = relPropNames;
  }

  /**
   * Gets relPropTypes
   *
   * @return value of relPropTypes
   */
  public String[] getRelPropTypes() {
    return relPropTypes;
  }

  /**
   * @param relPropTypes The relPropTypes to set
   */
  public void setRelPropTypes( String[] relPropTypes ) {
    this.relPropTypes = relPropTypes;
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key The key to set
   */
  public void setKey( String key ) {
    this.key = key;
  }

  /**
   * Gets relationship
   *
   * @return value of relationship
   */
  public String getRelationship() {
    return relationship;
  }

  /**
   * @param relationship The relationship to set
   */
  public void setRelationship( String relationship ) {
    this.relationship = relationship;
  }

  /**
   * Gets fromNodeLabelValues
   *
   * @return value of fromNodeLabelValues
   */
  public String[] getFromNodeLabelValues() {
    return fromNodeLabelValues;
  }

  /**
   * @param fromNodeLabelValues The fromNodeLabelValues to set
   */
  public void setFromNodeLabelValues( String[] fromNodeLabelValues ) {
    this.fromNodeLabelValues = fromNodeLabelValues;
  }

  /**
   * Gets toNodeLabelValues
   *
   * @return value of toNodeLabelValues
   */
  public String[] getToNodeLabelValues() {
    return toNodeLabelValues;
  }

  /**
   * @param toNodeLabelValues The toNodeLabelValues to set
   */
  public void setToNodeLabelValues( String[] toNodeLabelValues ) {
    this.toNodeLabelValues = toNodeLabelValues;
  }

  /**
   * Gets relationshipValue
   *
   * @return value of relationshipValue
   */
  public String getRelationshipValue() {
    return relationshipValue;
  }

  /**
   * @param relationshipValue The relationshipValue to set
   */
  public void setRelationshipValue( String relationshipValue ) {
    this.relationshipValue = relationshipValue;
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
   * Gets readOnlyFromNode
   *
   * @return value of readOnlyFromNode
   */
  public boolean isReadOnlyFromNode() {
    return readOnlyFromNode;
  }

  /**
   * @param readOnlyFromNode The readOnlyFromNode to set
   */
  public void setReadOnlyFromNode( boolean readOnlyFromNode ) {
    this.readOnlyFromNode = readOnlyFromNode;
  }

  /**
   * Gets readOnlyToNode
   *
   * @return value of readOnlyToNode
   */
  public boolean isReadOnlyToNode() {
    return readOnlyToNode;
  }

  /**
   * @param readOnlyToNode The readOnlyToNode to set
   */
  public void setReadOnlyToNode( boolean readOnlyToNode ) {
    this.readOnlyToNode = readOnlyToNode;
  }
}
