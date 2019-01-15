package bi.know.kettle.neo4j.steps.importer;

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
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@Step(
  id = "Neo4jImport",
  name = "Neo4j Import",
  description = "Runs an import command using the provided CSV files ",
  image = "neo4j_import.svg",
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class ImporterMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String FILENAME_FIELD = "filename_field_name";
  public static final String FILE_TYPE_FIELD = "file_type_field_name";
  public static final String ADMIN_COMMAND = "admin_command";
  public static final String BASE_FOLDER = "base_folder";
  public static final String REPORT_FILE = "report_file";
  public static final String DB_NAME = "db_name";
  public static final String IGNORE_DUPLICATE_NODES = "ignore_duplicate_nodes";
  public static final String IGNORE_MISSING_NODES = "ignore_missing_nodes";
  public static final String IGNORE_EXTRA_COLUMNS = "ignore_extra_columns";
  public static final String MAX_MEMORY = "max_memory";
  public static final String HIGH_IO = "high_io";
  public static final String MULTI_LINE = "multi_line";
  public static final String SKIP_BAD_RELATIONSHIPS= "skip_bad_relationships";
  public static final String READ_BUFFER_SIZE= "read_buffer_size";

  protected String filenameField;
  protected String fileTypeField;
  protected String baseFolder;
  protected String adminCommand;
  protected String reportFile;
  protected String databaseFilename;
  protected String maxMemory;
  protected boolean ignoringDuplicateNodes;
  protected boolean ignoringMissingNodes;
  protected boolean ignoringExtraColumns;
  protected boolean highIo;
  protected boolean multiLine;
  protected boolean skippingBadRelationships;
  protected String readBufferSize;


  @Override public void setDefault() {
    databaseFilename = "graph.db";
    adminCommand = "neo4j-import";
    baseFolder = "/var/lib/neo4j/";
    reportFile = "import.report";
    readBufferSize = "4M";
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    // No fields are added by default
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( FILENAME_FIELD, filenameField ) );
    xml.append( XMLHandler.addTagValue( FILE_TYPE_FIELD , fileTypeField) );
    xml.append( XMLHandler.addTagValue( ADMIN_COMMAND, adminCommand ) );
    xml.append( XMLHandler.addTagValue( BASE_FOLDER, baseFolder ) );
    xml.append( XMLHandler.addTagValue( REPORT_FILE, reportFile ) );
    xml.append( XMLHandler.addTagValue( DB_NAME, databaseFilename ) );
    xml.append( XMLHandler.addTagValue( MAX_MEMORY, maxMemory) );
    xml.append( XMLHandler.addTagValue( IGNORE_DUPLICATE_NODES, ignoringDuplicateNodes) );
    xml.append( XMLHandler.addTagValue( IGNORE_MISSING_NODES, ignoringMissingNodes) );
    xml.append( XMLHandler.addTagValue( IGNORE_EXTRA_COLUMNS, ignoringExtraColumns) );
    xml.append( XMLHandler.addTagValue( HIGH_IO, highIo) );
    xml.append( XMLHandler.addTagValue( MULTI_LINE, multiLine) );
    xml.append( XMLHandler.addTagValue( SKIP_BAD_RELATIONSHIPS, skippingBadRelationships) );
    xml.append( XMLHandler.addTagValue( READ_BUFFER_SIZE, readBufferSize) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    filenameField = XMLHandler.getTagValue( stepnode, FILENAME_FIELD );
    fileTypeField = XMLHandler.getTagValue( stepnode, FILE_TYPE_FIELD );
    adminCommand = XMLHandler.getTagValue( stepnode, ADMIN_COMMAND );
    baseFolder = XMLHandler.getTagValue( stepnode, BASE_FOLDER );
    reportFile = XMLHandler.getTagValue( stepnode, REPORT_FILE );
    databaseFilename = XMLHandler.getTagValue( stepnode, DB_NAME );
    maxMemory = XMLHandler.getTagValue( stepnode, MAX_MEMORY );
    ignoringDuplicateNodes = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, IGNORE_DUPLICATE_NODES ) );
    ignoringMissingNodes = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, IGNORE_MISSING_NODES ) );
    ignoringExtraColumns = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, IGNORE_EXTRA_COLUMNS ) );
    highIo = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, HIGH_IO ) );
    multiLine = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, MULTI_LINE ) );
    skippingBadRelationships = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, SKIP_BAD_RELATIONSHIPS) );
    readBufferSize = XMLHandler.getTagValue( stepnode, READ_BUFFER_SIZE);
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, FILENAME_FIELD, filenameField );
    rep.saveStepAttribute( transformationId, stepId, FILE_TYPE_FIELD, fileTypeField );
    rep.saveStepAttribute( transformationId, stepId, ADMIN_COMMAND, adminCommand );
    rep.saveStepAttribute( transformationId, stepId, BASE_FOLDER, baseFolder );
    rep.saveStepAttribute( transformationId, stepId, REPORT_FILE, reportFile );
    rep.saveStepAttribute( transformationId, stepId, DB_NAME, databaseFilename );
    rep.saveStepAttribute( transformationId, stepId, MAX_MEMORY, maxMemory );
    rep.saveStepAttribute( transformationId, stepId, IGNORE_DUPLICATE_NODES, ignoringDuplicateNodes );
    rep.saveStepAttribute( transformationId, stepId, IGNORE_MISSING_NODES, ignoringMissingNodes );
    rep.saveStepAttribute( transformationId, stepId, IGNORE_EXTRA_COLUMNS, ignoringExtraColumns );
    rep.saveStepAttribute( transformationId, stepId, HIGH_IO, highIo );
    rep.saveStepAttribute( transformationId, stepId, MULTI_LINE, multiLine);
    rep.saveStepAttribute( transformationId, stepId, SKIP_BAD_RELATIONSHIPS, skippingBadRelationships);
    rep.saveStepAttribute( transformationId, stepId, READ_BUFFER_SIZE, readBufferSize);
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    filenameField = rep.getStepAttributeString( stepId, FILENAME_FIELD );
    fileTypeField = rep.getStepAttributeString( stepId, FILE_TYPE_FIELD );
    adminCommand = rep.getStepAttributeString( stepId, ADMIN_COMMAND );
    baseFolder = rep.getStepAttributeString( stepId, BASE_FOLDER );
    reportFile = rep.getStepAttributeString( stepId, REPORT_FILE );
    databaseFilename = rep.getStepAttributeString( stepId, DB_NAME );
    maxMemory = rep.getStepAttributeString( stepId, MAX_MEMORY );
    ignoringDuplicateNodes = rep.getStepAttributeBoolean( stepId, IGNORE_DUPLICATE_NODES);
    ignoringMissingNodes = rep.getStepAttributeBoolean( stepId, IGNORE_MISSING_NODES);
    ignoringExtraColumns = rep.getStepAttributeBoolean( stepId, IGNORE_EXTRA_COLUMNS);
    highIo = rep.getStepAttributeBoolean( stepId, HIGH_IO);
    multiLine = rep.getStepAttributeBoolean( stepId, MULTI_LINE);
    skippingBadRelationships = rep.getStepAttributeBoolean( stepId, SKIP_BAD_RELATIONSHIPS);
    readBufferSize = rep.getStepAttributeString( stepId, READ_BUFFER_SIZE);
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new Importer( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new ImporterData();
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
   * Gets adminCommand
   *
   * @return value of adminCommand
   */
  public String getAdminCommand() {
    return adminCommand;
  }

  /**
   * @param adminCommand The adminCommand to set
   */
  public void setAdminCommand( String adminCommand ) {
    this.adminCommand = adminCommand;
  }

  /**
   * Gets reportFile
   *
   * @return value of reportFile
   */
  public String getReportFile() {
    return reportFile;
  }

  /**
   * @param reportFile The reportFile to set
   */
  public void setReportFile( String reportFile ) {
    this.reportFile = reportFile;
  }

  /**
   * Gets maxMemory
   *
   * @return value of maxMemory
   */
  public String getMaxMemory() {
    return maxMemory;
  }

  /**
   * @param maxMemory The maxMemory to set
   */
  public void setMaxMemory( String maxMemory ) {
    this.maxMemory = maxMemory;
  }

  /**
   * Gets ignoringDuplicateNodes
   *
   * @return value of ignoringDuplicateNodes
   */
  public boolean isIgnoringDuplicateNodes() {
    return ignoringDuplicateNodes;
  }

  /**
   * @param ignoringDuplicateNodes The ignoringDuplicateNodes to set
   */
  public void setIgnoringDuplicateNodes( boolean ignoringDuplicateNodes ) {
    this.ignoringDuplicateNodes = ignoringDuplicateNodes;
  }

  /**
   * Gets ignoringMissingNodes
   *
   * @return value of ignoringMissingNodes
   */
  public boolean isIgnoringMissingNodes() {
    return ignoringMissingNodes;
  }

  /**
   * @param ignoringMissingNodes The ignoringMissingNodes to set
   */
  public void setIgnoringMissingNodes( boolean ignoringMissingNodes ) {
    this.ignoringMissingNodes = ignoringMissingNodes;
  }

  /**
   * Gets ignoringExtraColumns
   *
   * @return value of ignoringExtraColumns
   */
  public boolean isIgnoringExtraColumns() {
    return ignoringExtraColumns;
  }

  /**
   * @param ignoringExtraColumns The ignoringExtraColumns to set
   */
  public void setIgnoringExtraColumns( boolean ignoringExtraColumns ) {
    this.ignoringExtraColumns = ignoringExtraColumns;
  }

  /**
   * Gets highIo
   *
   * @return value of highIo
   */
  public boolean isHighIo() {
    return highIo;
  }

  /**
   * @param highIo The highIo to set
   */
  public void setHighIo( boolean highIo ) {
    this.highIo = highIo;
  }

  /**
   * Gets databaseFilename
   *
   * @return value of databaseFilename
   */
  public String getDatabaseFilename() {
    return databaseFilename;
  }

  /**
   * @param databaseFilename The databaseFilename to set
   */
  public void setDatabaseFilename( String databaseFilename ) {
    this.databaseFilename = databaseFilename;
  }

  /**
   * Gets multiLine
   *
   * @return value of multiLine
   */
  public boolean isMultiLine() {
    return multiLine;
  }

  /**
   * @param multiLine The multiLine to set
   */
  public void setMultiLine( boolean multiLine ) {
    this.multiLine = multiLine;
  }

  /**
   * Gets skippingBadRelationships
   *
   * @return value of skippingBadRelationships
   */
  public boolean isSkippingBadRelationships() {
    return skippingBadRelationships;
  }

  /**
   * @param skippingBadRelationships The skippingBadRelationships to set
   */
  public void setSkippingBadRelationships( boolean skippingBadRelationships ) {
    this.skippingBadRelationships = skippingBadRelationships;
  }

  /**
   * Gets readBufferSize
   *
   * @return value of readBufferSize
   */
  public String getReadBufferSize() {
    return readBufferSize;
  }

  /**
   * @param readBufferSize The readBufferSize to set
   */
  public void setReadBufferSize( String readBufferSize ) {
    this.readBufferSize = readBufferSize;
  }
}
