package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

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

  @MetaStoreAttribute
  private String connectionLivenessCheckTimeout;

  @MetaStoreAttribute
  private String maxConnectionLifetime;

  @MetaStoreAttribute
  private String maxConnectionPoolSize;

  @MetaStoreAttribute
  private String connectionAcquisitionTimeout;

  @MetaStoreAttribute
  private String connectionTimeout;

  @MetaStoreAttribute
  private String maxTransactionRetryTime;


  public NeoConnection() {
    boltPort = "7687";
    browserPort = "7474";
  }

  public NeoConnection( VariableSpace parent ) {
    super.initializeVariablesFrom( parent );
    usingEncryption = true;
  }

  public NeoConnection( VariableSpace parent, NeoConnection source ) {
    this( parent );
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

      throw new Exception( "Unable to connect to database '" + name + "' : "+e.getMessage() , e );
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
    String realPassword = Encr.decryptPasswordOptionallyEncrypted(environmentSubstitute( password ));
    Config.ConfigBuilder configBuilder;
    if ( usingEncryption ) {
      configBuilder = Config.build().withEncryption();
    } else {
      configBuilder = Config.build().withoutEncryption();
    }

    if ( StringUtils.isNotEmpty( connectionLivenessCheckTimeout ) ) {
      long seconds = Const.toLong( environmentSubstitute( connectionLivenessCheckTimeout ), -1L );
      if ( seconds > 0 ) {
        configBuilder = configBuilder.withConnectionLivenessCheckTimeout( seconds, TimeUnit.MILLISECONDS );
      }
    }
    if ( StringUtils.isNotEmpty( maxConnectionLifetime ) ) {
      long seconds = Const.toLong( environmentSubstitute( maxConnectionLifetime ), -1L );
      if ( seconds > 0 ) {
        configBuilder = configBuilder.withMaxConnectionLifetime( seconds, TimeUnit.MILLISECONDS );
      }
    }
    if ( StringUtils.isNotEmpty( maxConnectionPoolSize ) ) {
      int size = Const.toInt( environmentSubstitute( maxConnectionPoolSize ), -1 );
      if ( size > 0 ) {
        configBuilder = configBuilder.withMaxConnectionPoolSize( size );
      }
    }
    if ( StringUtils.isNotEmpty( connectionAcquisitionTimeout ) ) {
      long seconds = Const.toLong( environmentSubstitute( connectionAcquisitionTimeout ), -1L );
      if ( seconds > 0 ) {
        configBuilder = configBuilder.withConnectionAcquisitionTimeout( seconds, TimeUnit.MILLISECONDS );
      }
    }
    if ( StringUtils.isNotEmpty( connectionTimeout ) ) {
      long seconds = Const.toLong( environmentSubstitute( connectionTimeout ), -1L );
      if ( seconds > 0 ) {
        configBuilder = configBuilder.withConnectionTimeout( seconds, TimeUnit.MILLISECONDS );
      }
    }
    if ( StringUtils.isNotEmpty( maxTransactionRetryTime ) ) {
      long seconds = Const.toLong( environmentSubstitute( maxTransactionRetryTime ), -1L );
      if ( seconds > 0 ) {
        configBuilder = configBuilder.withMaxTransactionRetryTime( seconds, TimeUnit.MILLISECONDS );
      }
    }

    Config config = configBuilder.toConfig();

    return GraphDatabase.driver( url, AuthTokens.basic( realUsername, realPassword ), config );
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

  /**
   * Gets connectionLivenessCheckTimeout
   *
   * @return value of connectionLivenessCheckTimeout
   */
  public String getConnectionLivenessCheckTimeout() {
    return connectionLivenessCheckTimeout;
  }

  /**
   * @param connectionLivenessCheckTimeout The connectionLivenessCheckTimeout to set
   */
  public void setConnectionLivenessCheckTimeout( String connectionLivenessCheckTimeout ) {
    this.connectionLivenessCheckTimeout = connectionLivenessCheckTimeout;
  }

  /**
   * Gets maxConnectionLifetime
   *
   * @return value of maxConnectionLifetime
   */
  public String getMaxConnectionLifetime() {
    return maxConnectionLifetime;
  }

  /**
   * @param maxConnectionLifetime The maxConnectionLifetime to set
   */
  public void setMaxConnectionLifetime( String maxConnectionLifetime ) {
    this.maxConnectionLifetime = maxConnectionLifetime;
  }

  /**
   * Gets maxConnectionPoolSize
   *
   * @return value of maxConnectionPoolSize
   */
  public String getMaxConnectionPoolSize() {
    return maxConnectionPoolSize;
  }

  /**
   * @param maxConnectionPoolSize The maxConnectionPoolSize to set
   */
  public void setMaxConnectionPoolSize( String maxConnectionPoolSize ) {
    this.maxConnectionPoolSize = maxConnectionPoolSize;
  }

  /**
   * Gets connectionAcquisitionTimeout
   *
   * @return value of connectionAcquisitionTimeout
   */
  public String getConnectionAcquisitionTimeout() {
    return connectionAcquisitionTimeout;
  }

  /**
   * @param connectionAcquisitionTimeout The connectionAcquisitionTimeout to set
   */
  public void setConnectionAcquisitionTimeout( String connectionAcquisitionTimeout ) {
    this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
  }

  /**
   * Gets connectionTimeout
   *
   * @return value of connectionTimeout
   */
  public String getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * @param connectionTimeout The connectionTimeout to set
   */
  public void setConnectionTimeout( String connectionTimeout ) {
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * Gets maxTransactionRetryTime
   *
   * @return value of maxTransactionRetryTime
   */
  public String getMaxTransactionRetryTime() {
    return maxTransactionRetryTime;
  }

  /**
   * @param maxTransactionRetryTime The maxTransactionRetryTime to set
   */
  public void setMaxTransactionRetryTime( String maxTransactionRetryTime ) {
    this.maxTransactionRetryTime = maxTransactionRetryTime;
  }
}
