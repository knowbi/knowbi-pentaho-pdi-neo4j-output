package bi.know.kettle.neo4j.steps.gencsv;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
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
  id = "Neo4jLoad",
  name = "Neo4j Generate CSVs",
  description = "Generate CSV files for nodes and relationships in the import/ folder for use with neo4j-import",
  image = "neo4j_load.svg",
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class GenerateCsvMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String GRAPH_FIELD_NAME = "graph_field_name";
  public static final String BASE_FOLDER = "base_folder";
  public static final String UNIQUENESS_STRATEGY = "uniqueness_strategy";
  public static final String FILES_PREFIX = "files_prefix";
  public static final String FILENAME_FIELD = "filename_field";
  public static final String FILE_TYPE_FIELD = "file_type_field";

  protected String graphFieldName;
  protected String baseFolder;
  protected UniquenessStrategy uniquenessStrategy;

  protected String filesPrefix;
  protected String filenameField;
  protected String fileTypeField;

  @Override public void setDefault() {
    baseFolder = "/var/lib/neo4j/";
    uniquenessStrategy = UniquenessStrategy.None;
    filesPrefix = "prefix";
    filenameField = "filename";
    fileTypeField = "fileType";
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore ) {

    inputRowMeta.clear();

    ValueMetaInterface filenameValueMeta = new ValueMetaString( space.environmentSubstitute( filenameField ) );
    filenameValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( filenameValueMeta );

    ValueMetaInterface fileTypeValueMeta = new ValueMetaString( space.environmentSubstitute( fileTypeField ) );
    fileTypeValueMeta.setOrigin( name );
    inputRowMeta.addValueMeta( fileTypeValueMeta );


  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( GRAPH_FIELD_NAME, graphFieldName ) );
    xml.append( XMLHandler.addTagValue( BASE_FOLDER, baseFolder ) );
    xml.append( XMLHandler.addTagValue( UNIQUENESS_STRATEGY, uniquenessStrategy != null ? uniquenessStrategy.name() : null ) );
    xml.append( XMLHandler.addTagValue( FILES_PREFIX, filesPrefix ) );
    xml.append( XMLHandler.addTagValue( FILENAME_FIELD, filenameField ) );
    xml.append( XMLHandler.addTagValue( FILE_TYPE_FIELD, fileTypeField ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    graphFieldName = XMLHandler.getTagValue( stepnode, GRAPH_FIELD_NAME );
    baseFolder = XMLHandler.getTagValue( stepnode, BASE_FOLDER );
    uniquenessStrategy = UniquenessStrategy.getStrategyFromName( XMLHandler.getTagValue( stepnode, UNIQUENESS_STRATEGY ) );
    filesPrefix = XMLHandler.getTagValue( stepnode, FILES_PREFIX );
    filenameField = XMLHandler.getTagValue( stepnode, FILENAME_FIELD );
    fileTypeField = XMLHandler.getTagValue( stepnode, FILE_TYPE_FIELD );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, GRAPH_FIELD_NAME, graphFieldName );
    rep.saveStepAttribute( transformationId, stepId, BASE_FOLDER, baseFolder );
    rep.saveStepAttribute( transformationId, stepId, UNIQUENESS_STRATEGY, uniquenessStrategy != null ? uniquenessStrategy.name() : null );
    rep.saveStepAttribute( transformationId, stepId, FILES_PREFIX, filesPrefix );
    rep.saveStepAttribute( transformationId, stepId, FILENAME_FIELD, filenameField );
    rep.saveStepAttribute( transformationId, stepId, FILE_TYPE_FIELD, fileTypeField );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    graphFieldName = rep.getStepAttributeString( stepId, GRAPH_FIELD_NAME );
    baseFolder = rep.getStepAttributeString( stepId, BASE_FOLDER );
    uniquenessStrategy = UniquenessStrategy.getStrategyFromName( rep.getStepAttributeString( stepId, UNIQUENESS_STRATEGY ) );
    filesPrefix = rep.getStepAttributeString( stepId, FILES_PREFIX );
    filenameField = rep.getStepAttributeString( stepId, FILENAME_FIELD );
    fileTypeField = rep.getStepAttributeString( stepId, FILE_TYPE_FIELD );
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new GenerateCsv( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new GenerateCsvData();
  }

  /**
   * Gets graphFieldName
   *
   * @return value of graphFieldName
   */
  public String getGraphFieldName() {
    return graphFieldName;
  }

  /**
   * @param graphFieldName The graphFieldName to set
   */
  public void setGraphFieldName( String graphFieldName ) {
    this.graphFieldName = graphFieldName;
  }


  /**
   * Gets baseFolder
   *
   * @return value of baseFolder
   */
  public String getBaseFolder() {
    return baseFolder;
  }

  /**
   * @param baseFolder The baseFolder to set
   */
  public void setBaseFolder( String baseFolder ) {
    this.baseFolder = baseFolder;
  }

  /**
   * Gets nodeUniquenessStrategy
   *
   * @return value of nodeUniquenessStrategy
   */
  public UniquenessStrategy getUniquenessStrategy() {
    return uniquenessStrategy;
  }

  /**
   * @param uniquenessStrategy The nodeUniquenessStrategy to set
   */
  public void setUniquenessStrategy( UniquenessStrategy uniquenessStrategy ) {
    this.uniquenessStrategy = uniquenessStrategy;
  }

  /**
   * Gets filesPrefix
   *
   * @return value of filesPrefix
   */
  public String getFilesPrefix() {
    return filesPrefix;
  }

  /**
   * @param filesPrefix The filesPrefix to set
   */
  public void setFilesPrefix( String filesPrefix ) {
    this.filesPrefix = filesPrefix;
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField The filenameField to set
   */
  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  /**
   * Gets fileTypeField
   *
   * @return value of fileTypeField
   */
  public String getFileTypeField() {
    return fileTypeField;
  }

  /**
   * @param fileTypeField The fileTypeField to set
   */
  public void setFileTypeField( String fileTypeField ) {
    this.fileTypeField = fileTypeField;
  }
}
