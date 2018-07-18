package bi.know.kettle.neo4j.steps.output;

import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class Neo4JOutputData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;

  public String[] fieldNames;

  public NeoConnection neoConnection;
  public String url;
  public Driver driver;
  public Session session;
  public Transaction transaction;

  public long batchSize;
  public long outputCount;

  public int[] fromNodePropIndexes;
  public int[] fromNodeLabelIndexes;
  public int[] toNodePropIndexes;
  public int[] toNodeLabelIndexes;
  public int[] relPropIndexes;
  public int relationshipIndex;
  public GraphPropertyType[] fromNodePropTypes;
  public GraphPropertyType[] toNodePropTypes;
  public GraphPropertyType[] relPropTypes;

  public List<Map<String, Object>> fromUnwindList;
  public String fromLabelsClause;
  public List<Map<String, Object>> toUnwindList;
  public String toLabelsClause;

  public IMetaStore metaStore;
}
