package bi.know.kettle.neo4j.steps.output;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
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
  image = "NEO4J.svg",
  i18nPackageName = "bi.know.kettle.neo4j.steps.output",
  name = "Neo4JOutput.Step.Name",
  description = "Neo4JOutput.Step.Description",
  categoryDescription = "Neo4JOutput.Step.Category"
)
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface {

  private static final String STRING_CONNECTION = "connection";
  private static final String STRING_BATCH_SIZE = "batch_size";
  private static final String STRING_KEY = "key";
  private static final String STRING_FROM = "from";
  private static final String STRING_LABELS = "labels";
  private static final String STRING_LABEL = "label";
  private static final String STRING_PROPERTIES = "properties";
  private static final String STRING_PROPERTY = "property";
  private static final String STRING_PROPERTY_NAME = "name";
  private static final String STRING_PROPERTY_VALUE = "value";
  private static final String STRING_PROPERTY_TYPE = "type";
  private static final String STRING_PROPERTY_PRIMARY = "primary";
  private static final String STRING_TO = "to";
  private static final String STRING_RELATIONSHIP = "relationship";
  private static final String STRING_RELPROPS = "relprops";
  private static final String STRING_RELPROP = "relprop";
  private static final String STRING_CREATE_INDEXES = "create_indexes";
  private static final String STRING_USE_CREATE = "use_create";

  private String connection;
  private String key;
  private String relationship;

  private String[] fromNodeProps;
  private String[] fromNodePropNames;
  private String[] fromNodePropTypes;
  private boolean[] fromNodePropPrimary;

  private String[] toNodeProps;
  private String[] toNodePropNames;
  private String[] toNodePropTypes;
  private boolean[] toNodePropPrimary;

  private String[] fromNodeLabels;
  private String[] toNodeLabels;
  private String[] relProps;
  private String[] relPropNames;
  private String[] relPropTypes;

  private String batchSize;
  private boolean creatingIndexes;
  private boolean usingCreate;


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

    fromNodeLabels = new String[ 0 ];

    fromNodeProps = new String[ 0 ];
    fromNodePropNames = new String[ 0 ];
    fromNodePropTypes = new String[ 0 ];
    fromNodePropPrimary = new boolean[ 0 ];

    toNodeLabels = new String[ 0 ];
    toNodeProps = new String[ 0 ];
    toNodePropNames = new String[ 0 ];
    toNodePropTypes = new String[ 0 ];
    toNodePropPrimary = new boolean[ 0 ];

    relProps = new String[ 0 ];
    relPropNames = new String[ 0 ];
    relPropTypes = new String[ 0 ];
  }

  public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();

    xml.append( XMLHandler.addTagValue( STRING_CONNECTION, connection ) );
    xml.append( XMLHandler.addTagValue( STRING_BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( STRING_KEY, key ) );
    xml.append( XMLHandler.addTagValue( STRING_CREATE_INDEXES, creatingIndexes ) );
    xml.append( XMLHandler.addTagValue( STRING_USE_CREATE, usingCreate ) );

    xml.append( XMLHandler.openTag( STRING_FROM ) );

    xml.append( XMLHandler.openTag( STRING_LABELS ) );
    for ( int i = 0; i < fromNodeLabels.length; i++ ) {
      xml.append( XMLHandler.addTagValue( STRING_LABEL, fromNodeLabels[ i ] ) );
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

    xml.append( XMLHandler.openTag( STRING_LABELS ) );
    for ( int i = 0; i < toNodeLabels.length; i++ ) {
      xml.append( XMLHandler.addTagValue( STRING_LABEL, toNodeLabels[ i ] ) );
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

    Node fromNode = XMLHandler.getSubNode( stepnode, STRING_FROM );
    Node fromLabelsNode = XMLHandler.getSubNode( fromNode, STRING_LABELS );
    List<Node> fromLabelNodes = XMLHandler.getNodes( fromLabelsNode, STRING_LABEL );

    fromNodeLabels = new String[ fromLabelNodes.size() ];

    for ( int i = 0; i < fromLabelNodes.size(); i++ ) {
      Node labelNode = fromLabelNodes.get( i );
      fromNodeLabels[ i ] = XMLHandler.getNodeValue( labelNode );
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
    Node toLabelsNode = XMLHandler.getSubNode( toNode, STRING_LABELS );
    List<Node> toLabelNodes = XMLHandler.getNodes( toLabelsNode, STRING_LABEL );

    toNodeLabels = new String[ toLabelNodes.size() ];

    for ( int i = 0; i < toLabelNodes.size(); i++ ) {
      Node labelNode = toLabelNodes.get( i );
      toNodeLabels[ i ] = XMLHandler.getNodeValue( labelNode );
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

    Node relPropsNode = XMLHandler.getSubNode( stepnode, STRING_RELPROPS );
    List<Node> relPropNodes = XMLHandler.getNodes( toPropsNode, STRING_RELPROP );

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
    // No fields added
  }

  public Object clone() {
    return super.clone();
  }

  public void setKey( String key ) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public void setFromNodeLabels( String[] fromNodeLabels ) {
    this.fromNodeLabels = fromNodeLabels;
  }

  public String[] getFromNodeLabels() {
    return fromNodeLabels;
  }

  public void setFromNodeProps( String[] fromNodeProps ) {
    this.fromNodeProps = fromNodeProps;
  }

  public void setFromNodePropNames( String[] fromNodePropNames ) {
    this.fromNodePropNames = fromNodePropNames;
  }

  public String[] getFromNodeProps() {
    return fromNodeProps;
  }

  public String[] getFromNodePropNames() {
    return fromNodePropNames;
  }

  public void setToNodeLabels( String[] toNodeLabels ) {
    this.toNodeLabels = toNodeLabels;
  }

  public String[] getToNodeLabels() {
    return toNodeLabels;
  }

  public void setToNodeProps( String[] toNodeProps ) {
    this.toNodeProps = toNodeProps;
  }

  public void setToNodePropNames( String[] toNodePropNames ) {
    this.toNodePropNames = toNodePropNames;
  }

  public String[] getToNodeProps() {
    return toNodeProps;
  }

  public String[] getToNodePropNames() {
    return toNodePropNames;
  }

  public void setRelationship( String relationship ) {
    this.relationship = relationship;
  }

  public String getRelationship() {
    return relationship;
  }

  public void setRelProps( String[] relProps ) {
    this.relProps = relProps;
  }

  public String[] getRelProps() {
    return relProps;
  }

  public void setRelPropNames( String[] relPropNames ) {
    this.relPropNames = relPropNames;
  }

  public String[] getRelPropNames() {
    return relPropNames;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection( String connection ) {
    this.connection = connection;
  }

  public String[] getFromNodePropTypes() {
    return fromNodePropTypes;
  }

  public void setFromNodePropTypes( String[] fromNodePropTypes ) {
    this.fromNodePropTypes = fromNodePropTypes;
  }

  public String[] getToNodePropTypes() {
    return toNodePropTypes;
  }

  public void setToNodePropTypes( String[] toNodePropTypes ) {
    this.toNodePropTypes = toNodePropTypes;
  }

  public String[] getRelPropTypes() {
    return relPropTypes;
  }

  public void setRelPropTypes( String[] relPropTypes ) {
    this.relPropTypes = relPropTypes;
  }

  public boolean[] getFromNodePropPrimary() {
    return fromNodePropPrimary;
  }

  public void setFromNodePropPrimary( boolean[] fromNodePropPrimary ) {
    this.fromNodePropPrimary = fromNodePropPrimary;
  }

  public boolean[] getToNodePropPrimary() {
    return toNodePropPrimary;
  }

  public void setToNodePropPrimary( boolean[] toNodePropPrimary ) {
    this.toNodePropPrimary = toNodePropPrimary;
  }

  public boolean isCreatingIndexes() {
    return creatingIndexes;
  }

  public void setCreatingIndexes( boolean creatingIndexes ) {
    this.creatingIndexes = creatingIndexes;
  }

  public boolean isUsingCreate() {
    return usingCreate;
  }

  public void setUsingCreate( boolean usingCreate ) {
    this.usingCreate = usingCreate;
  }

}
