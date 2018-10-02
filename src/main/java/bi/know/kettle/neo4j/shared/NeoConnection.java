package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.net.URLEncoder;

@MetaStoreElementType( name = "Neo4j Connection", description = "A shared connection to a Neo4j server" )
public class NeoConnection extends Variables {
  private String name;

  @MetaStoreAttribute
  private String server;

  @MetaStoreAttribute
  private String boltPort;

  @MetaStoreAttribute
  private String browserPort;

  @MetaStoreAttribute
  private boolean routing;

  @MetaStoreAttribute
  private String routingPolicy;

  @MetaStoreAttribute
  private String username;

  @MetaStoreAttribute( password = true )
  private String password;

  @MetaStoreAttribute
  private boolean usingEncryption;

  public NeoConnection() {
    boltPort = "7687";
    browserPort = "7474";
  }

  public NeoConnection( VariableSpace parent ) {
    super.initializeVariablesFrom( parent );
    usingEncryption = true;
  }

  public NeoConnection(VariableSpace parent, NeoConnection source) {
    this(parent);
    this.name = source.name;
    this.server = source.server;
    this.boltPort = source.boltPort;
    this.browserPort = source.browserPort;
    this.routing = source.routing;
    this.routingPolicy = source.routingPolicy;
    this.username = source.username;
    this.password = source.password;
    this.usingEncryption = source.usingEncryption;
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override
  public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override
  public boolean equals( Object object ) {

    if ( object == this ) {
      return true;
    }
    if ( !( object instanceof NeoConnection ) ) {
      return false;
    }

    NeoConnection connection = (NeoConnection) object;

    return name != null && name.equalsIgnoreCase( connection.name );
  }

  /**
   * Test this connection to Neo4j
   *
   * @throws Exception In case anything goes wrong
   */
  public void test() throws Exception {

    try {
      Driver driver = getDriver( LogChannel.GENERAL );
      Session session = driver.session();
      session.close();
    } catch ( Exception e ) {
      throw new Exception( "Unable to connect to database '" + name + '"', e );
    }
  }

  public String getUrl() {

    /*
     * Construct the following URL:
     *
     * bolt://hostname:port
     * bolt+routing://core-server:port/?policy=MyPolicy
     */
    String url = "bolt";

    if ( routing ) {
      url += "+routing";
    }

    url += "://";

    // Hostname
    //
    url += environmentSubstitute( server );

    // Port
    //
    url += ":" + environmentSubstitute( boltPort );

    String routingPolicyString = environmentSubstitute( routingPolicy );
    if ( routing && StringUtils.isNotEmpty( routingPolicyString ) ) {
      try {
        url += "?policy=" + URLEncoder.encode( routingPolicyString, "UTF-8" );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error encoding routing policy context '" + routingPolicyString + "' in connection URL", e );
        url += "?policy=" + routingPolicyString;
      }
    }

    return url;
  }

  public Driver getDriver( LogChannelInterface log ) {
    String url = getUrl();
    String realUsername = environmentSubstitute( username );
    String realPassword = environmentSubstitute( password );
    if ( usingEncryption ) {
      return GraphDatabase.driver( url, AuthTokens.basic( realUsername, realPassword ) );
    } else {
      return GraphDatabase.driver( url, AuthTokens.basic( realUsername, realPassword ), Config.build().withoutEncryption().toConfig() );
    }
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets server
   *
   * @return value of server
   */
  public String getServer() {
    return server;
  }

  /**
   * @param server The server to set
   */
  public void setServer( String server ) {
    this.server = server;
  }

  /**
   * Gets boltPort
   *
   * @return value of boltPort
   */
  public String getBoltPort() {
    return boltPort;
  }

  /**
   * @param boltPort The boltPort to set
   */
  public void setBoltPort( String boltPort ) {
    this.boltPort = boltPort;
  }

  /**
   * Gets browserPort
   *
   * @return value of browserPort
   */
  public String getBrowserPort() {
    return browserPort;
  }

  /**
   * @param browserPort The browserPort to set
   */
  public void setBrowserPort( String browserPort ) {
    this.browserPort = browserPort;
  }

  /**
   * Gets routing
   *
   * @return value of routing
   */
  public boolean isRouting() {
    return routing;
  }

  /**
   * @param routing The routing to set
   */
  public void setRouting( boolean routing ) {
    this.routing = routing;
  }

  /**
   * Gets routingPolicy
   *
   * @return value of routingPolicy
   */
  public String getRoutingPolicy() {
    return routingPolicy;
  }

  /**
   * @param routingPolicy The routingPolicy to set
   */
  public void setRoutingPolicy( String routingPolicy ) {
    this.routingPolicy = routingPolicy;
  }

  /**
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username The username to set
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * Gets usingEncryption
   *
   * @return value of usingEncryption
   */
  public boolean isUsingEncryption() {
    return usingEncryption;
  }

  /**
   * @param usingEncryption The usingEncryption to set
   */
  public void setUsingEncryption( boolean usingEncryption ) {
    this.usingEncryption = usingEncryption;
  }
}
