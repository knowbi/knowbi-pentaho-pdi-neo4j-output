package bi.know.kettle.neo4j.steps.split;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
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

import java.util.List;

@Step(
  id = "Neo4jSplitGraph",
  name = "Neo4j Split Graph",
  description = "Splits the nodes and relationships of a graph data type",
  image = "neo4j_split.svg",
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class SplitGraphMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String GRAPH_FIELD = "graph_field";
  public static final String TYPE_FIELD = "type_field";
  public static final String ID_FIELD = "id_field";
  public static final String PROPERTY_SET_FIELD = "property_set_field";

  protected String graphField;
  protected String typeField;
  protected String idField;
  protected String propertySetField;

  @Override public void setDefault() {
    graphField = "graph";
    typeField = "type";
    idField = "id";
    propertySetField = "propertySet";
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    if ( StringUtils.isNotEmpty( typeField ) ) {
      ValueMetaString typeValueMeta = new ValueMetaString(space.environmentSubstitute( typeField ));
      typeValueMeta.setOrigin( name );
      inputRowMeta.addValueMeta( typeValueMeta );
    }
    if ( StringUtils.isNotEmpty( idField ) ) {
      ValueMetaString idValueMeta = new ValueMetaString(space.environmentSubstitute( idField ));
      idValueMeta.setOrigin( name );
      inputRowMeta.addValueMeta( idValueMeta );
    }
    if ( StringUtils.isNotEmpty( propertySetField ) ) {
      ValueMetaString propertySetValueMeta = new ValueMetaString(space.environmentSubstitute( propertySetField ));
      propertySetValueMeta.setOrigin( name );
      inputRowMeta.addValueMeta( propertySetValueMeta );
    }
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( GRAPH_FIELD, graphField ) );
    xml.append( XMLHandler.addTagValue( TYPE_FIELD, typeField ) );
    xml.append( XMLHandler.addTagValue( ID_FIELD, idField ) );
    xml.append( XMLHandler.addTagValue( PROPERTY_SET_FIELD, propertySetField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    graphField = XMLHandler.getTagValue( stepnode, GRAPH_FIELD );
    typeField = XMLHandler.getTagValue( stepnode, TYPE_FIELD );
    idField = XMLHandler.getTagValue( stepnode, ID_FIELD );
    propertySetField = XMLHandler.getTagValue( stepnode, PROPERTY_SET_FIELD);
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, GRAPH_FIELD, graphField );
    rep.saveStepAttribute( transformationId, stepId, TYPE_FIELD, typeField );
    rep.saveStepAttribute( transformationId, stepId, ID_FIELD, idField );
    rep.saveStepAttribute( transformationId, stepId, PROPERTY_SET_FIELD, propertySetField );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    graphField = rep.getStepAttributeString( stepId, GRAPH_FIELD );
    typeField = rep.getStepAttributeString( stepId, TYPE_FIELD );
    idField = rep.getStepAttributeString( stepId, ID_FIELD );
    propertySetField = rep.getStepAttributeString( stepId, PROPERTY_SET_FIELD );
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new SplitGraph( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new SplitGraphData();
  }

  /**
   * Gets graphField
   *
   * @return value of graphField
   */
  public String getGraphField() {
    return graphField;
  }

  /**
   * @param graphField The graphField to set
   */
  public void setGraphField( String graphField ) {
    this.graphField = graphField;
  }

  /**
   * Gets typeField
   *
   * @return value of typeField
   */
  public String getTypeField() {
    return typeField;
  }

  /**
   * @param typeField The typeField to set
   */
  public void setTypeField( String typeField ) {
    this.typeField = typeField;
  }

  /**
   * Gets idField
   *
   * @return value of idField
   */
  public String getIdField() {
    return idField;
  }

  /**
   * @param idField The idField to set
   */
  public void setIdField( String idField ) {
    this.idField = idField;
  }

  /**
   * Gets propertySetField
   *
   * @return value of propertySetField
   */
  public String getPropertySetField() {
    return propertySetField;
  }

  /**
   * @param propertySetField The propertySetField to set
   */
  public void setPropertySetField( String propertySetField ) {
    this.propertySetField = propertySetField;
  }
}
