package bi.know.kettle.neo4j.entries.check;

import bi.know.kettle.neo4j.shared.MetaStoreUtil;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.kettle.core.Neo4jDefaults;
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
import org.neo4j.kettle.core.metastore.MetaStoreFactory;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@JobEntry(
  id = "NEO4J_CHECK_CONNECTIONS",
  name = "Check Neo4j Connections",
  description = "Check to see if we can connecto to the listed Neo4j databases",
  image = "neo4j_check.svg",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.Conditions",
  documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/"
)
public class CheckConnections extends JobEntryBase implements JobEntryInterface {

  private List<String> connectionNames;

  public CheckConnections() {
    this.connectionNames = new ArrayList<>();
  }

  public CheckConnections( String name ) {
    this( name, "" );
  }

  public CheckConnections( String name, String description ) {
    super( name, description );
    connectionNames = new ArrayList<>();
  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    // Add entry name, type, ...
    //
    xml.append( super.getXML() );

    xml.append( XMLHandler.openTag( "connections" ) );

    for ( String connectionName : connectionNames ) {
      xml.append( XMLHandler.addTagValue( "connection", connectionName ) );
    }

    xml.append( XMLHandler.closeTag( "connections" ) );
    return xml.toString();
  }

  @Override public void loadXML( Node node, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep, IMetaStore metaStore ) throws KettleXMLException {

    super.loadXML( node, databases, slaveServers );

    connectionNames = new ArrayList<>();
    Node connectionsNode = XMLHandler.getSubNode( node, "connections" );
    List<Node> connectionNodes = XMLHandler.getNodes( connectionsNode, "connection" );
    for ( Node connectionNode : connectionNodes ) {
      String connectionName = XMLHandler.getNodeValue( connectionNode );
      connectionNames.add( connectionName );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId jobId ) throws KettleException {
    for ( int i = 0; i < connectionNames.size(); i++ ) {
      rep.saveJobEntryAttribute( jobId, getObjectId(), i, "connection", connectionNames.get( i ) );
    }
  }

  @Override public void loadRep( Repository rep, IMetaStore metaStore, ObjectId jobEntryId, List<DatabaseMeta> databases, List<SlaveServer> slaveServers ) throws KettleException {
    connectionNames = new ArrayList<>();
    int nrConnections = rep.countNrJobEntryAttributes( jobEntryId, "connection" );
    for ( int i = 0; i < nrConnections; i++ ) {
      connectionNames.add( rep.getJobEntryAttributeString( jobEntryId, i, "connection" ) );
    }
  }

  @Override public Result execute( Result result, int nr ) throws KettleException {

    try {
      metaStore = MetaStoreUtil.findMetaStore( this );
    } catch ( Exception e ) {
      throw new KettleException( "Error finding metastore", e );
    }
    MetaStoreFactory<NeoConnection> connectionFactory = new MetaStoreFactory<>( NeoConnection.class, metaStore, Neo4jDefaults.NAMESPACE );

    // Replace variables & parameters
    //
    List<String> realConnectionNames = new ArrayList<>();
    for ( String connectionName : connectionNames ) {
      realConnectionNames.add( environmentSubstitute( connectionName ) );
    }

    // Check all the connections.  If any one fails, fail the step
    // Check 'm all though, report on all, nr of errors is nr of failed connections
    //
    int testCount = 0;
    for ( String connectionName : realConnectionNames ) {
      testCount++;
      try {
        NeoConnection connection = connectionFactory.loadElement( connectionName );
        if ( connection == null ) {
          throw new KettleException( "Unable to find connection with name '" + connectionName + "'" );
        }
        connection.initializeVariablesFrom( this );

        Session session = connection.getSession( log );
        session.close();

      } catch ( Exception e ) {
        // Something bad happened, log the error, flag error
        //
        result.increaseErrors( 1 );
        result.setResult( false );
        logError( "Error on connection: " + connectionName, e );
      }
    }

    if ( result.getNrErrors() == 0 ) {
      logBasic( testCount + " Neo4j connections tested without error" );
    } else {
      logBasic( testCount + " Neo4j connections tested with " + result.getNrErrors() + " error(s)" );
    }

    return result;
  }

  @Override public String getDialogClassName() {
    return super.getDialogClassName();
  }

  /**
   * Gets connectionNames
   *
   * @return value of connectionNames
   */
  public List<String> getConnectionNames() {
    return connectionNames;
  }

  /**
   * @param connectionNames The connectionNames to set
   */
  public void setConnectionNames( List<String> connectionNames ) {
    this.connectionNames = connectionNames;
  }

  @Override public boolean evaluates() {
    return true;
  }

  @Override public boolean isUnconditional() {
    return false;
  }
}
