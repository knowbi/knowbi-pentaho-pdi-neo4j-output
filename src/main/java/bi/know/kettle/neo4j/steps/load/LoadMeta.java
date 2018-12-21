package bi.know.kettle.neo4j.steps.load;

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
  id = "Neo4jLoad",
  name = "Neo4j Load",
  description = "Loads graph data into a Neo4j database using neo4j-admin import",
  image = "NEO4J.svg",
  categoryDescription = "Neo4j",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class LoadMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String CONNECTION_NAME = "connection_name";
  public static final String GRAPH_FIELD_NAME = "graph_field_name";
  public static final String DB_NAME = "db_name";
  public static final String ADMIN_COMMAND = "admin_command";
  public static final String BASE_FOLDER = "base_folder";
  public static final String UNIQUENESS_STRATEGY = "uniqueness_strategy";
  public static final String REPORT_FILE = "report_file";

  protected String connectionName;

  protected String graphFieldName;

  protected String databaseFilename;

  protected String baseFolder;

  protected String adminCommand;

  protected UniquenessStrategy uniquenessStrategy;

  protected String reportFile;

  @Override public void setDefault() {
    databaseFilename = "graph.db";
    adminCommand = "neo4j-admin";
    baseFolder = "/var/lib/neo4j/";
    uniquenessStrategy = UniquenessStrategy.None;
    reportFile = "import.report";
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore )
    throws KettleStepException {
    // No fields are added by default
  }

  @Override public String getXML() throws KettleException {
    StringBuffer xml = new StringBuffer();
    xml.append( XMLHandler.addTagValue( CONNECTION_NAME, connectionName ) );
    xml.append( XMLHandler.addTagValue( GRAPH_FIELD_NAME, graphFieldName ) );
    xml.append( XMLHandler.addTagValue( DB_NAME, databaseFilename ) );
    xml.append( XMLHandler.addTagValue( ADMIN_COMMAND, adminCommand ) );
    xml.append( XMLHandler.addTagValue( BASE_FOLDER, baseFolder ) );
    xml.append( XMLHandler.addTagValue( UNIQUENESS_STRATEGY, uniquenessStrategy!=null ? uniquenessStrategy.name() : null) );
    xml.append( XMLHandler.addTagValue( REPORT_FILE, reportFile ) );
    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, CONNECTION_NAME );
    graphFieldName = XMLHandler.getTagValue( stepnode, GRAPH_FIELD_NAME );
    databaseFilename = XMLHandler.getTagValue( stepnode, DB_NAME );
    adminCommand = XMLHandler.getTagValue( stepnode, ADMIN_COMMAND );
    baseFolder = XMLHandler.getTagValue( stepnode, BASE_FOLDER );
    uniquenessStrategy = UniquenessStrategy.getStrategyFromName( XMLHandler.getTagValue( stepnode, UNIQUENESS_STRATEGY ) );
    reportFile = XMLHandler.getTagValue( stepnode, REPORT_FILE);
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, CONNECTION_NAME, connectionName );
    rep.saveStepAttribute( transformationId, stepId, GRAPH_FIELD_NAME, graphFieldName );
    rep.saveStepAttribute( transformationId, stepId, DB_NAME, databaseFilename );
    rep.saveStepAttribute( transformationId, stepId, ADMIN_COMMAND, adminCommand );
    rep.saveStepAttribute( transformationId, stepId, BASE_FOLDER, baseFolder );
    rep.saveStepAttribute( transformationId, stepId, UNIQUENESS_STRATEGY, uniquenessStrategy!=null ? uniquenessStrategy.name() : null);
    rep.saveStepAttribute( transformationId, stepId, REPORT_FILE, reportFile);
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( stepId, CONNECTION_NAME );
    graphFieldName = rep.getStepAttributeString( stepId, GRAPH_FIELD_NAME );
    databaseFilename = rep.getStepAttributeString( stepId, DB_NAME );
    adminCommand = rep.getStepAttributeString( stepId, ADMIN_COMMAND );
    baseFolder = rep.getStepAttributeString( stepId, BASE_FOLDER );
    uniquenessStrategy = UniquenessStrategy.getStrategyFromName( rep.getStepAttributeString( stepId, UNIQUENESS_STRATEGY) );
    reportFile = rep.getStepAttributeString( stepId, REPORT_FILE);
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    return new Load( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new LoadData();
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
}
