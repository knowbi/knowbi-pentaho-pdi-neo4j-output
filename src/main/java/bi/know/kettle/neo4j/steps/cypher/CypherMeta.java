package bi.know.kettle.neo4j.steps.cypher;

import bi.know.kettle.neo4j.model.GraphPropertyType;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
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

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "Neo4jCypherOutput",
  name = "Neo4j Cypher",
  description = "Reads from or writes to Neo4j using Cypher with parameter data from input fields",
  image = "neo4j_cypher.svg",
  categoryDescription = "Neo4j"
)
public class CypherMeta extends BaseStepMeta implements StepMetaInterface {

  private String connectionName;
  private String cypher;
  private String batchSize;
  private boolean cypherFromField;
  private String cypherField;
  private List<ParameterMapping> parameterMappings;
  private List<ReturnValue> returnValues;

 public CypherMeta() {
   super();
   parameterMappings = new ArrayList<>();
   returnValues = new ArrayList<>();
  }

  @Override public void setDefault() {

  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new Cypher( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new CypherData();
  }

  @Override public String getDialogClassName() {
    return CypherDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException  {

    // Check return values in the metadata...
    for (ReturnValue returnValue : returnValues) {
      try {
        int type = ValueMetaFactory.getIdForValueMeta( returnValue.getType() );
        ValueMetaInterface valueMeta =  ValueMetaFactory.createValueMeta( returnValue.getName(), type);
        valueMeta.setOrigin( name );
        rowMeta.addValueMeta( valueMeta );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unknown data type '"+returnValue.getType()+"' for value named '"+returnValue.getName()+"'" );
      }
    }


   // No output fields for now
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder( );
    xml.append( XMLHandler.addTagValue( "connection", connectionName) );
    xml.append( XMLHandler.addTagValue( "cypher", cypher) );
    xml.append( XMLHandler.addTagValue( "batch_size", batchSize) );
    xml.append( XMLHandler.addTagValue( "cypher_from_field", cypherFromField) );
    xml.append( XMLHandler.addTagValue( "cypher_field", cypherField) );

    xml.append( XMLHandler.openTag( "mappings") );
    for (ParameterMapping parameterMapping : parameterMappings) {
      xml.append( XMLHandler.openTag( "mapping") );
      xml.append( XMLHandler.addTagValue( "parameter", parameterMapping.getParameter()) );
      xml.append( XMLHandler.addTagValue( "field", parameterMapping.getField() ) );
      xml.append( XMLHandler.addTagValue( "type", parameterMapping.getNeoType() ) );
      xml.append( XMLHandler.closeTag( "mapping") );
    }
    xml.append( XMLHandler.closeTag( "mappings") );

    xml.append( XMLHandler.openTag( "returns") );
    for (ReturnValue returnValue : returnValues) {
      xml.append( XMLHandler.openTag( "return") );
      xml.append( XMLHandler.addTagValue( "name", returnValue.getName()) );
      xml.append( XMLHandler.addTagValue( "type", returnValue.getType()) );
      xml.append( XMLHandler.closeTag( "return") );
    }
    xml.append( XMLHandler.closeTag( "returns") );


    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, "connection" );
    cypher = XMLHandler.getTagValue( stepnode, "cypher" );
    batchSize = XMLHandler.getTagValue( stepnode, "batch_size" );
    cypherFromField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "cypher_from_field" ));
    cypherField = XMLHandler.getTagValue( stepnode, "cypher_field" );

    // Parse parameter mappings
    //
    Node mappingsNode = XMLHandler.getSubNode( stepnode, "mappings" );
    List<Node> mappingNodes = XMLHandler.getNodes( mappingsNode, "mapping" );
    parameterMappings = new ArrayList<>();
    for (Node mappingNode : mappingNodes) {
      String parameter = XMLHandler.getTagValue( mappingNode, "parameter" );
      String field = XMLHandler.getTagValue( mappingNode, "field" );
      String neoType = XMLHandler.getTagValue( mappingNode, "type" );
      if ( StringUtils.isEmpty(neoType)) {
        neoType = GraphPropertyType.String.name();
      }
      parameterMappings.add(new ParameterMapping( parameter, field, neoType ));
    }

    // Parse return values
    //
    Node returnsNode = XMLHandler.getSubNode( stepnode, "returns" );
    List<Node> returnNodes = XMLHandler.getNodes( returnsNode, "return" );
    returnValues = new ArrayList<>();
    for (Node returnNode : returnNodes) {
      String name = XMLHandler.getTagValue( returnNode, "name" );
      String type = XMLHandler.getTagValue( returnNode, "type" );
      returnValues.add(new ReturnValue( name, type));
    }

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "connection", connectionName);
    rep.saveStepAttribute( id_transformation, id_step, "cypher", cypher);
    rep.saveStepAttribute( id_transformation, id_step, "batch_size", batchSize);
    rep.saveStepAttribute( id_transformation, id_step, "cypher_from_field", cypherFromField);
    rep.saveStepAttribute( id_transformation, id_step, "cypher_field", cypherField);

    for (int i=0;i<parameterMappings.size();i++) {
      ParameterMapping parameterMapping = parameterMappings.get( i );
      rep.saveStepAttribute( id_transformation, id_step, i, "parameter_name",  parameterMapping.getParameter());
      rep.saveStepAttribute( id_transformation, id_step, i, "parameter_field",  parameterMapping.getField() );
      rep.saveStepAttribute( id_transformation, id_step, i, "parameter_type",  parameterMapping.getNeoType() );
    }
    for (int i=0;i<returnValues.size();i++) {
      ReturnValue returnValue = returnValues.get( i );
      rep.saveStepAttribute( id_transformation, id_step, i, "return_name",  returnValue.getName());
      rep.saveStepAttribute( id_transformation, id_step, i, "return_type",  returnValue.getType());
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( id_step, "connection" );
    cypher = rep.getStepAttributeString( id_step, "cypher" );
    batchSize = rep.getStepAttributeString( id_step, "batch_size" );
    cypherFromField = rep.getStepAttributeBoolean( id_step, "cypher_from_field" );
    cypherField = rep.getStepAttributeString( id_step, "cypher_field" );

    parameterMappings = new ArrayList<>();
    int nrMappings = rep.countNrStepAttributes( id_step, "parameter" );
    for (int i=0;i<nrMappings;i++) {
      String parameter = rep.getStepAttributeString( id_step, i, "parameter_name" );
      String field = rep.getStepAttributeString( id_step, i, "parameter_field" );
      String neoType = rep.getStepAttributeString( id_step, i, "parameter_type" );
      if ( StringUtils.isEmpty(neoType)) {
        neoType = GraphPropertyType.String.name();
      }
      parameterMappings.add( new ParameterMapping( parameter, field, neoType) );
    }
    returnValues = new ArrayList<>();
    int nrReturns = rep.countNrStepAttributes( id_step, "return_name" );
    for (int i=0;i<nrReturns;i++) {
      String name = rep.getStepAttributeString( id_step, i, "return_name" );
      String type = rep.getStepAttributeString( id_step, i, "return_type" );
      returnValues.add(new ReturnValue( name, type ));
    }

  }

  public String getConnectionName() {
    return connectionName;
  }

  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }

  public String getCypher() {
    return cypher;
  }

  public void setCypher( String cypher ) {
    this.cypher = cypher;
  }

  public List<ParameterMapping> getParameterMappings() {
    return parameterMappings;
  }

  public void setParameterMappings( List<ParameterMapping> parameterMappings ) {
    this.parameterMappings = parameterMappings;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }

  public void setReturnValues( List<ReturnValue> returnValues ) {
    this.returnValues = returnValues;
  }

  public boolean isCypherFromField() {
    return cypherFromField;
  }

  public void setCypherFromField( boolean cypherFromField ) {
    this.cypherFromField = cypherFromField;
  }

  public String getCypherField() {
    return cypherField;
  }

  public void setCypherField( String cypherField ) {
    this.cypherField = cypherField;
  }
}
