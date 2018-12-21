package bi.know.kettle.neo4j.steps.load;

import bi.know.kettle.neo4j.shared.NeoConnection;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class LoadData extends BaseStepData implements StepDataInterface {

  public NeoConnection connection;
  public MetaStoreFactory<NeoConnection> connectionFactory;
  public IMetaStore metaStore;
  public Driver driver;
  public Session session;
  public String importFolder;
  public long nodesProcessed;
  public long relsProcessed;
  public int graphFieldIndex;
  public IndexedGraphData indexedGraphData;
  public String adminCommand;
  public String databaseFilename;
  public String baseFolder;
  public String nodeFilename;
  public OutputStream nodeOutputStream;
  public OutputStream relsOutputStream;
  public List<IdType> nodeProps;
  public List<IdType> relProps;
  public Map<String, Integer> nodePropertyIndexes;
  public Map<String, Integer> relPropertyIndexes;
  public String reportFile;
  public String relsFilename;
}
