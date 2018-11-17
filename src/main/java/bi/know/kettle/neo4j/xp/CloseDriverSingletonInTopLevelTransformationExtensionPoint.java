package bi.know.kettle.neo4j.xp;

import bi.know.kettle.neo4j.core.Neo4jDefaults;
import bi.know.kettle.neo4j.shared.DriverSingleton;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobAdapter;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransAdapter;

@ExtensionPoint(
  id = "CloseDriverSingletonInTopLevelTransformationExtensionPoint",
  extensionPointId = "TransformationStart",
  description = "Close the Neo4j driver singleton at the end of a top level transformation"
)
public class CloseDriverSingletonInTopLevelTransformationExtensionPoint implements ExtensionPointInterface {

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {

    if ( !( object instanceof Trans ) ) {
      return; // not for us
    }

    Trans trans = (Trans) object;

    trans.addTransListener( new TransAdapter() {
      @Override public void transFinished( Trans trans ) throws KettleException {
        // Only close if we're in a top level transformation and not running on a container
        //
        if (trans.getContainerObjectId()==null && trans.getParentJob()==null && trans.getParentTrans()==null) {
          String cleanup = trans.getVariable( Neo4jDefaults.VARIABLE_NEO4J_CLEANUP_DRIVERS );
          if (!StringUtils.isEmpty( cleanup ) && ( "Y".equalsIgnoreCase( cleanup  ) || "Yes".equalsIgnoreCase( cleanup ) || "TRUE".equalsIgnoreCase( cleanup ))  ) {
            DriverSingleton.closeAll();
          }
        }
      }
    } );
  }
}
