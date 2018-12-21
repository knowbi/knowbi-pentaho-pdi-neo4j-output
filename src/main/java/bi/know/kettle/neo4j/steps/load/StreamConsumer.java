package bi.know.kettle.neo4j.steps.load;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogLevel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamConsumer extends Thread {

  private LogChannelInterface log;
  private InputStream inputStream;
  private final LogLevel logLevel;

  public StreamConsumer( LogChannelInterface log, InputStream inputStream, LogLevel logLevel ) {
    super();
    this.log = log;
    this.inputStream = inputStream;
    this.logLevel = logLevel;
  }

  @Override public void run() {

    try {
      InputStreamReader inputStreamReader = new InputStreamReader( inputStream );
      BufferedReader bufferedReader = new BufferedReader( inputStreamReader );
      String line = null;
      while ((line=bufferedReader.readLine())!=null) {
        switch(logLevel){
          case MINIMAL: log.logMinimal(line); break;
          case BASIC: log.logBasic(line); break;
          case DETAILED: log.logDetailed(line); break;
          case DEBUG: log.logDebug(line); break;
          case ROWLEVEL: log.logRowlevel(line); break;
          case ERROR: log.logError(line); break;
          case NOTHING:
          default:
            break;
        }
      }
    } catch ( IOException e ) {
      log.logError("Error consuming thread:", e);
    }

  }
}
