package bi.know.kettle.neo4j.steps.cypher;

import bi.know.kettle.neo4j.core.value.ValueMetaGraph;
import bi.know.kettle.neo4j.model.GraphPropertyType;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
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
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/Neo4j-Cypher#description"
)
@InjectionSupported( localizationPrefix = "Cypher.Injection.", groups = { "PARAMETERS", "RETURNS" } )
public class CypherMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String CONNECTION = "connection";
  public static final String CYPHER = "cypher";
  public static final String BATCH_SIZE = "batch_size";
  public static final String CYPHER_FROM_FIELD = "cypher_from_field";
  public static final String CYPHER_FIELD = "cypher_field";
  public static final String UNWIND = "unwind";
  public static final String UNWIND_MAP = "unwind_map";
  public static final String RETURNING_GRAPH = "returning_graph";
  public static final String RETURN_GRAPH_FIELD = "return_graph_field";
  public static final String MAPPINGS = "mappings";
  public static final String MAPPING = "mapping";
  public static final String PARAMETERS = "parameters";
  public static final String PARAMETER = "parameter";
  public static final String FIELD = "field";
  public static final String TYPE = "type";
  public static final String RETURNS = "returns";
  public static final String RETURN = "return";
  public static final String NAME = "name";
  public static final String PARAMETER_NAME = "parameter_name";
  public static final String PARAMETER_FIELD = "parameter_field";
  public static final String PARAMETER_TYPE = "parameter_type";

  public static final String RETURN_NAME = "return_name";
  public static final String RETURN_TYPE = "return_type";

  @Injection( name = CONNECTION )
  private String connectionName;

  @Injection( name = CYPHER )
  private String cypher;

  @Injection( name = BATCH_SIZE )
  private String batchSize;

  @Injection( name = CYPHER_FROM_FIELD )
  private boolean cypherFromField;

  @Injection( name = CYPHER_FIELD )
  private String cypherField;

  @Injection( name = UNWIND )
  private boolean usingUnwind;

  @Injection( name = UNWIND_MAP )
  private String unwindMapName;

  @Injection( name = RETURNING_GRAPH )
  private boolean returningGraph;

  @Injection( name = RETURN_GRAPH_FIELD )
  private String returnGraphField;

  @InjectionDeep
  private List<ParameterMapping> parameterMappings;

  @InjectionDeep
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
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {

    if ( usingUnwind ) {
      // Unwind only outputs results, not input
      //
      rowMeta.clear();
    }

    if (returningGraph) {
      // We send out a single Graph value per input row
      //
      ValueMetaInterface valueMetaGraph = new ValueMetaGraph( Const.NVL(returnGraphField, "graph") );
      valueMetaGraph.setOrigin( name );
      rowMeta.addValueMeta( valueMetaGraph );
    } else {
      // Check return values in the metadata...
      //
      for ( ReturnValue returnValue : returnValues ) {
        try {
          int type = ValueMetaFactory.getIdForValueMeta( returnValue.getType() );
          ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( returnValue.getName(), type );
          valueMeta.setOrigin( name );
          rowMeta.addValueMeta( valueMeta );
        } catch ( KettlePluginException e ) {
          throw new KettleStepException( "Unknown data type '" + returnValue.getType() + "' for value named '" + returnValue.getName() + "'" );
        }
      }
    }
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( CONNECTION, connectionName ) );
    xml.append( XMLHandler.addTagValue( CYPHER, cypher ) );
    xml.append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    xml.append( XMLHandler.addTagValue( CYPHER_FROM_FIELD, cypherFromField ) );
    xml.append( XMLHandler.addTagValue( CYPHER_FIELD, cypherField ) );
    xml.append( XMLHandler.addTagValue( UNWIND, usingUnwind ) );
    xml.append( XMLHandler.addTagValue( UNWIND_MAP, unwindMapName ) );
    xml.append( XMLHandler.addTagValue( RETURNING_GRAPH, returningGraph ) );
    xml.append( XMLHandler.addTagValue( RETURN_GRAPH_FIELD, returnGraphField) );

    xml.append( XMLHandler.openTag( MAPPINGS ) );
    for ( ParameterMapping parameterMapping : parameterMappings ) {
      xml.append( XMLHandler.openTag( MAPPING ) );
      xml.append( XMLHandler.addTagValue( PARAMETER, parameterMapping.getParameter() ) );
      xml.append( XMLHandler.addTagValue( FIELD, parameterMapping.getField() ) );
      xml.append( XMLHandler.addTagValue( TYPE, parameterMapping.getNeoType() ) );
      xml.append( XMLHandler.closeTag( MAPPING ) );
    }
    xml.append( XMLHandler.closeTag( MAPPINGS ) );

    xml.append( XMLHandler.openTag( RETURNS ) );
    for ( ReturnValue returnValue : returnValues ) {
      xml.append( XMLHandler.openTag( RETURN ) );
      xml.append( XMLHandler.addTagValue( NAME, returnValue.getName() ) );
      xml.append( XMLHandler.addTagValue( TYPE, returnValue.getType() ) );
      xml.append( XMLHandler.closeTag( RETURN ) );
    }
    xml.append( XMLHandler.closeTag( RETURNS ) );


    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, CONNECTION );
    cypher = XMLHandler.getTagValue( stepnode, CYPHER );
    batchSize = XMLHandler.getTagValue( stepnode, BATCH_SIZE );
    cypherFromField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, CYPHER_FROM_FIELD ) );
    cypherField = XMLHandler.getTagValue( stepnode, CYPHER_FIELD );
    usingUnwind = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, UNWIND ) );
    unwindMapName = XMLHandler.getTagValue( stepnode, UNWIND_MAP );
    returningGraph = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, RETURNING_GRAPH ) );
    returnGraphField = XMLHandler.getTagValue( stepnode, RETURN_GRAPH_FIELD);

    // Parse parameter mappings
    //
    Node mappingsNode = XMLHandler.getSubNode( stepnode, MAPPINGS );
    List<Node> mappingNodes = XMLHandler.getNodes( mappingsNode, MAPPING );
    parameterMappings = new ArrayList<>();
    for ( Node mappingNode : mappingNodes ) {
      String parameter = XMLHandler.getTagValue( mappingNode, PARAMETER );
      String field = XMLHandler.getTagValue( mappingNode, FIELD );
      String neoType = XMLHandler.getTagValue( mappingNode, TYPE );
      if ( StringUtils.isEmpty( neoType ) ) {
        neoType = GraphPropertyType.String.name();
      }
      parameterMappings.add( new ParameterMapping( parameter, field, neoType ) );
    }

    // Parse return values
    //
    Node returnsNode = XMLHandler.getSubNode( stepnode, RETURNS );
    List<Node> returnNodes = XMLHandler.getNodes( returnsNode, RETURN );
    returnValues = new ArrayList<>();
    for ( Node returnNode : returnNodes ) {
      String name = XMLHandler.getTagValue( returnNode, NAME );
      String type = XMLHandler.getTagValue( returnNode, TYPE );
      returnValues.add( new ReturnValue( name, type ) );
    }

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, CONNECTION, connectionName );
    rep.saveStepAttribute( transformationId, stepId, CYPHER, cypher );
    rep.saveStepAttribute( transformationId, stepId, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transformationId, stepId, CYPHER_FROM_FIELD, cypherFromField );
    rep.saveStepAttribute( transformationId, stepId, CYPHER_FIELD, cypherField );
    rep.saveStepAttribute( transformationId, stepId, UNWIND, usingUnwind );
    rep.saveStepAttribute( transformationId, stepId, UNWIND_MAP, unwindMapName );
    rep.saveStepAttribute( transformationId, stepId, RETURNING_GRAPH, returningGraph );
    rep.saveStepAttribute( transformationId, stepId, RETURN_GRAPH_FIELD, returnGraphField );

    for ( int i = 0; i < parameterMappings.size(); i++ ) {
      ParameterMapping parameterMapping = parameterMappings.get( i );
      rep.saveStepAttribute( transformationId, stepId, i, PARAMETER_NAME, parameterMapping.getParameter() );
      rep.saveStepAttribute( transformationId, stepId, i, PARAMETER_FIELD, parameterMapping.getField() );
      rep.saveStepAttribute( transformationId, stepId, i, PARAMETER_TYPE, parameterMapping.getNeoType() );
    }
    for ( int i = 0; i < returnValues.size(); i++ ) {
      ReturnValue returnValue = returnValues.get( i );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_NAME, returnValue.getName() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_TYPE, returnValue.getType() );
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( stepId, CONNECTION );
    cypher = rep.getStepAttributeString( stepId, CYPHER );
    batchSize = rep.getStepAttributeString( stepId, BATCH_SIZE );
    cypherFromField = rep.getStepAttributeBoolean( stepId, CYPHER_FROM_FIELD );
    cypherField = rep.getStepAttributeString( stepId, CYPHER_FIELD );
    usingUnwind = rep.getStepAttributeBoolean( stepId, UNWIND );
    unwindMapName = rep.getStepAttributeString( stepId, UNWIND_MAP );
    returningGraph = rep.getStepAttributeBoolean( stepId, RETURNING_GRAPH );
    returnGraphField = rep.getStepAttributeString( stepId, RETURN_GRAPH_FIELD );

    parameterMappings = new ArrayList<>();
    int nrMappings = rep.countNrStepAttributes( stepId, PARAMETER );
    for ( int i = 0; i < nrMappings; i++ ) {
      String parameter = rep.getStepAttributeString( stepId, i, PARAMETER_NAME );
      String field = rep.getStepAttributeString( stepId, i, PARAMETER_FIELD );
      String neoType = rep.getStepAttributeString( stepId, i, PARAMETER_TYPE );
      if ( StringUtils.isEmpty( neoType ) ) {
        neoType = GraphPropertyType.String.name();
      }
      parameterMappings.add( new ParameterMapping( parameter, field, neoType ) );
    }
    returnValues = new ArrayList<>();
    int nrReturns = rep.countNrStepAttributes( stepId, RETURN_NAME );
    for ( int i = 0; i < nrReturns; i++ ) {
      String name = rep.getStepAttributeString( stepId, i, RETURN_NAME );
      String type = rep.getStepAttributeString( stepId, i, RETURN_TYPE );
      returnValues.add( new ReturnValue( name, type ) );
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
   * Gets cypher
   *
   * @return value of cypher
   */
  public String getCypher() {
    return cypher;
  }

  /**
   * @param cypher The cypher to set
   */
  public void setCypher( String cypher ) {
    this.cypher = cypher;
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
   * Gets cypherFromField
   *
   * @return value of cypherFromField
   */
  public boolean isCypherFromField() {
    return cypherFromField;
  }

  /**
   * @param cypherFromField The cypherFromField to set
   */
  public void setCypherFromField( boolean cypherFromField ) {
    this.cypherFromField = cypherFromField;
  }

  /**
   * Gets cypherField
   *
   * @return value of cypherField
   */
  public String getCypherField() {
    return cypherField;
  }

  /**
   * @param cypherField The cypherField to set
   */
  public void setCypherField( String cypherField ) {
    this.cypherField = cypherField;
  }

  /**
   * Gets usingUnwind
   *
   * @return value of usingUnwind
   */
  public boolean isUsingUnwind() {
    return usingUnwind;
  }

  /**
   * @param usingUnwind The usingUnwind to set
   */
  public void setUsingUnwind( boolean usingUnwind ) {
    this.usingUnwind = usingUnwind;
  }

  /**
   * Gets unwindMapName
   *
   * @return value of unwindMapName
   */
  public String getUnwindMapName() {
    return unwindMapName;
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
   * @param unwindMapName The unwindMapName to set
   */
  public void setUnwindMapName( String unwindMapName ) {
    this.unwindMapName = unwindMapName;
  }

  /**
   * Gets parameterMappings
   *
   * @return value of parameterMappings
   */
  public List<ParameterMapping> getParameterMappings() {
    return parameterMappings;
  }

  /**
   * @param parameterMappings The parameterMappings to set
   */
  public void setParameterMappings( List<ParameterMapping> parameterMappings ) {
    this.parameterMappings = parameterMappings;
  }

  /**
   * Gets returnValues
   *
   * @return value of returnValues
   */
  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }

  /**
   * @param returnValues The returnValues to set
   */
  public void setReturnValues( List<ReturnValue> returnValues ) {
    this.returnValues = returnValues;
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
}
