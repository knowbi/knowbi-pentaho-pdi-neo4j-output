package bi.know.kettle.neo4j.steps.importer;

import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.steps.load.IdType;
import bi.know.kettle.neo4j.steps.load.IndexedGraphData;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class ImporterData extends BaseStepData implements StepDataInterface {

  public List<String> nodesFiles;
  public List<String> relsFiles;

  public String importFolder;
  public String adminCommand;
  public String databaseFilename;
  public String baseFolder;
  public String reportFile;


  public int filenameFieldIndex;
  public int fileTypeFieldIndex;
}
