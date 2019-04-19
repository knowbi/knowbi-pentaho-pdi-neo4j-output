package bi.know.kettle.neo4j.steps.output;

import bi.know.kettle.neo4j.steps.BaseNeoStepData;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.kettle.model.GraphPropertyType;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class Neo4JOutputData extends BaseNeoStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;

  public String[] fieldNames;

  public NeoConnection neoConnection;
  public String url;
  public Driver driver;
  public Session session;

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

  public List<Map<String, Object>> unwindList;

  public String fromLabelsClause;
  public String toLabelsClause;
  public String[] fromLabelValues;
  public String[] toLabelValues;
  public String relationshipLabelValue;

  public String previousFromLabelsClause;
  public String previousToLabelsClause;
  public String previousRelationshipLabel;

  public IMetaStore metaStore;
  public boolean dynamicFromLabels;
  public boolean dynamicToLabels;
  public boolean dynamicRelLabel;
  public String relationshipLabel;
  public List<String> fromLabels;
  public List<String> toLabels;


  public OperationType fromOperationType;
  public OperationType toOperationType;
  public OperationType relOperationType;

  public String cypher;
}
