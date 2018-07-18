package bi.know.kettle.neo4j.core;

import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;

public class MetaStoreUtil {

  public static final IMetaStore findMetaStore( LoggingObjectInterface executor ) {

    if ( executor instanceof StepInterface ) {
      StepInterface step = (StepInterface) executor;
      if ( step.getMetaStore() != null ) {
        return step.getMetaStore();
      }
      Trans trans = step.getTrans();
      if ( trans != null ) {
        if ( trans.getMetaStore() != null ) {
          return trans.getMetaStore();
        }
        if ( trans.getTransMeta().getMetaStore() != null ) {
          return trans.getTransMeta().getMetaStore();
        }
      }
    }

    if ( executor instanceof Trans ) {
      Trans trans = (Trans) executor;
      if ( trans.getMetaStore() != null ) {
        return trans.getMetaStore();
      }
      if ( trans.getTransMeta().getMetaStore() != null ) {
        return trans.getMetaStore();
      }
    }

    if ( executor instanceof Job ) {
      Job job = (Job) executor;
      if ( job.getJobMeta().getMetaStore() != null ) {
        return job.getJobMeta().getMetaStore();
      }
    }

    LoggingObjectInterface parent = executor.getParent();
    if ( parent != null ) {
      IMetaStore metaStore = findMetaStore( parent );
      if ( metaStore != null ) {
        return metaStore;
      }
    }

    // Didn't find it anywhere in the tree above: lazy programmers!
    //
    return null;
  }
}
