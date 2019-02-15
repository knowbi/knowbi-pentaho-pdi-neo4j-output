package bi.know.kettle.neo4j.steps.graph;

import bi.know.kettle.neo4j.model.GraphModel;
import bi.know.kettle.neo4j.model.GraphProperty;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.steps.BaseNeoStepData;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.HashMap;
import java.util.Map;

public class GraphOutputData extends BaseNeoStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public NeoConnection neoConnection;
  public String url;
  public Driver driver;
  public Session session;
  public int[] fieldIndexes;
  public long batchSize;
  public Transaction transaction;
  public long outputCount;
  public boolean hasInput;
  public GraphModel graphModel;
  public int nodeCount;
  public IMetaStore metaStore;
  public Map<String, CypherParameters> cypherMap;
  public HashMap<String, Map<GraphProperty, Integer>> relationshipPropertyIndexMap;
}
