package bi.know.kettle.neo4j.steps.cypher;

import bi.know.kettle.neo4j.shared.NeoConnection;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class CypherData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public NeoConnection neoConnection;
  public String url;
  public Driver driver;
  public Session session;
  public int[] fieldIndexes;
  public String cypher;
  public long batchSize;
  public Transaction transaction;
  public long outputCount;
  public boolean hasInput;
  public int cypherFieldIndex;
}
