package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

public class MetaStoreUtil {

  public static final IMetaStore findMetaStore( LoggingObjectInterface executor ) throws MetaStoreException {

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
    System.err.println("METASTORE PROBLEM: Local couldn't be found, force local anyway");

    return MetaStoreConst.openLocalPentahoMetaStore();
  }

  public static final String getMetaStoreDescription(IMetaStore metaStore) throws MetaStoreException {
    String name = metaStore.getName();
    String desc = metaStore.getDescription();

    String description = "";
    if (metaStore instanceof DelegatingMetaStore ) {
      DelegatingMetaStore delegatingMetaStore = (DelegatingMetaStore) metaStore;
      boolean first = true;
      for (IMetaStore store : delegatingMetaStore.getMetaStoreList()) {
        if (first) {
          description+="{ ";
        } else {
          description+=", ";
        }
        description+=getMetaStoreDescription( store );
        if (first) {
          description+=" }";
          first = false;
        }
      }
    } else if (metaStore instanceof XmlMetaStore ) {
      String rootFolder = ((XmlMetaStore)metaStore).getRootFolder();
      description += name + "( located in folder: " + rootFolder + " )";
    } else {
      if ( StringUtils.isNotEmpty(desc)) {
        description += name + "(" + desc + ")";
      } else {
        description += name;
      }
    }
    return description;
  }
}
