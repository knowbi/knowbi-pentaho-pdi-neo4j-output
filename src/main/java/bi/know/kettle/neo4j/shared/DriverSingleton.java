package bi.know.kettle.neo4j.shared;

import org.neo4j.driver.v1.Driver;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriverSingleton {

  private static DriverSingleton singleton;

  private Map<String, org.neo4j.driver.v1.Driver> driverMap;

  private DriverSingleton() {
    driverMap = new HashMap<>(  );
  }

  public static DriverSingleton getInstance() {
    if (singleton==null) {
      singleton = new DriverSingleton();
    }
    return singleton;
  }

  public static Driver getDriver( LogChannelInterface log, NeoConnection connection) {
    DriverSingleton ds = getInstance();

    String key = getDriverKey( connection );

    Driver driver = ds.driverMap.get(key);
    if (driver==null) {
      driver = connection.getDriver( log );
      ds.driverMap.put(key, driver);
    }

    return driver;
  }

  public static void closeAll() {
    DriverSingleton ds = getInstance();

    List<String> keys = new ArrayList<>( ds.getDriverMap().keySet() );
    for (String key : keys) {
      synchronized ( ds.getDriverMap() ) {
        Driver driver = ds.driverMap.get( key );
        driver.close();
        ds.driverMap.remove( key );
      }
    }
  }

  private static String getDriverKey(NeoConnection connection) {
    String hostname = connection.environmentSubstitute( connection.getServer() );
    String boltPort = connection.environmentSubstitute( connection.getBoltPort() );
    String username = connection.environmentSubstitute( connection.getUsername() );

    return hostname+":"+boltPort+"@"+username;
  }

  /**
   * Gets driverMap
   *
   * @return value of driverMap
   */
  public Map<String, Driver> getDriverMap() {
    return driverMap;
  }


}
