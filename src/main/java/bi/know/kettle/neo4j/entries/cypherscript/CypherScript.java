package bi.know.kettle.neo4j.entries.cypherscript;

import bi.know.kettle.neo4j.shared.MetaStoreUtil;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.kettle.core.Neo4jDefaults;
import org.neo4j.kettle.core.metastore.MetaStoreFactory;
import org.neo4j.kettle.shared.DriverSingleton;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@JobEntry(
  id="NEO4J_CYPHER_SCRIPT",
  name="Neo4j Cypher Script",
  description = "Execute a Neo4j Cypher script",
  image="neo4j_cypher.svg",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.Scripting",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class CypherScript extends JobEntryBase implements JobEntryInterface {

  private String connectionName;

  private String script;

  private boolean replacingVariables;

  public CypherScript() {
    this("", "");
  }

  public CypherScript( String name) {
    this( name, "" );
  }

  public CypherScript( String name, String description ) {
    super( name, description );
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    // Add entry name, type, ...
    //
    xml.append( super.getXML() );

    xml.append( XMLHandler.addTagValue( "connection", connectionName ) );
    xml.append( XMLHandler.addTagValue( "script", script) );
    xml.append( XMLHandler.addTagValue( "replace_variables", replacingVariables ? "Y" : "N") );

    return xml.toString();
  }

  @Override public void loadXML( Node node, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep, IMetaStore metaStore ) throws KettleXMLException {

    super.loadXML( node, databases, slaveServers );

    connectionName = XMLHandler.getTagValue( node, "connection" );
    script = XMLHandler.getTagValue( node, "script" );
    replacingVariables =  "Y".equalsIgnoreCase( XMLHandler.getTagValue( node, "replace_variables" ) );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId jobId ) throws KettleException {
    rep.saveJobEntryAttribute( jobId, getObjectId(), "connection", connectionName );
    rep.saveJobEntryAttribute( jobId, getObjectId(), "script", script );
    rep.saveJobEntryAttribute( jobId, getObjectId(), "replace_variables", replacingVariables);
  }

  @Override public void loadRep( Repository rep, IMetaStore metaStore, ObjectId jobEntryId, List<DatabaseMeta> databases, List<SlaveServer> slaveServers ) throws KettleException {
    connectionName = rep.getJobEntryAttributeString( jobEntryId, "connection" );
    script = rep.getJobEntryAttributeString( jobEntryId, "script" );
    replacingVariables = rep.getJobEntryAttributeBoolean( jobEntryId, "replace_variables" );
  }

  @Override public Result execute( Result result, int nr ) throws KettleException {

    try {
      metaStore = MetaStoreUtil.findMetaStore( this );
    } catch(Exception e) {
      throw new KettleException( "Error finding metastore", e );
    }
    MetaStoreFactory<NeoConnection> connectionFactory = new MetaStoreFactory<>( NeoConnection.class, metaStore, Neo4jDefaults.NAMESPACE );

    // Replace variables & parameters
    //
    NeoConnection connection;
    String realConnectionName = environmentSubstitute( connectionName );
    try {
      if (StringUtils.isEmpty( realConnectionName )) {
        throw new KettleException( "The Neo4j connection name is not set" );
      }

      connection = connectionFactory.loadElement( realConnectionName );
      if (connection==null) {
        throw new KettleException( "Unable to find connection with name '"+realConnectionName+"'" );
      }
    } catch(Exception e) {
      result.setResult( false );
      result.increaseErrors( 1L );
      throw new KettleException( "Unable to gencsv or find connection with name '"+realConnectionName+"'", e);
    }

    String realScript;
    if (replacingVariables) {
      realScript = environmentSubstitute( script );
    } else {
      realScript = script;
    }

    // Share variables with the connection metadata
    //
    connection.initializeVariablesFrom( this );

    Session session = null;
    Transaction transaction = null;
    int nrExecuted = 0;
    try {

      // Connect to the database
      //
      session = connection.getSession(log);
      transaction = session.beginTransaction();

      // Split the script into parts : semi-colon at the start of a separate line
      //
      String[] commands = realScript.split( "\\r?\\n;" );
      for ( String command : commands ) {
        // Cleanup command: replace leading and trailing whitespaces and newlines
        //
        String cypher = command
          .replaceFirst( "^\\s+", "" )
          .replaceFirst( "\\s+$", "" );

        // Only execute if the statement is not empty
        //
        if ( StringUtils.isNotEmpty( cypher ) ) {
          transaction.run( cypher );
          nrExecuted++;
          log.logDetailed("Executed cypher statement: "+cypher);
        }
      }

      // Commit
      //
      transaction.commit();
    } catch(Exception e) {
      // Error connecting or executing
      // Roll back
      if (transaction!=null) {
        transaction.rollback();
      }
      result.increaseErrors( 1L );
      result.setResult( false );
      log.logError("Error executing statements:", e);
    } finally {
      // Clean up transaction, session and driver
      //
      if (transaction!=null) {
        transaction.close();
      }
      if (session!=null) {
        session.close();
      }
    }

    if (result.getNrErrors()==0) {
      logBasic("Neo4j script executed "+nrExecuted+" statements without error");
    } else {
      logBasic("Neo4j script executed with error(s)");
    }

    return result;
  }

  @Override public String getDialogClassName() {
    return super.getDialogClassName();
  }

  @Override public boolean evaluates() {
    return true;
  }

  @Override public boolean isUnconditional() {
    return false;
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
   * Gets script
   *
   * @return value of script
   */
  public String getScript() {
    return script;
  }

  /**
   * @param script The script to set
   */
  public void setScript( String script ) {
    this.script = script;
  }

  /**
   * Gets replacingVariables
   *
   * @return value of replacingVariables
   */
  public boolean isReplacingVariables() {
    return replacingVariables;
  }

  /**
   * @param replacingVariables The replacingVariables to set
   */
  public void setReplacingVariables( boolean replacingVariables ) {
    this.replacingVariables = replacingVariables;
  }
}
