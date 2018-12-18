package bi.know.kettle.neo4j.steps.dimensiongraph;

import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.steps.BaseNeoStepData;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.util.Date;
import java.util.List;

public class DimensionGraphData extends BaseNeoStepData implements StepDataInterface {

    public IMetaStore metaStore;

    public NeoConnection neoConnection;
    public String url;
    public Driver driver;
    public Session session;
    public Transaction transaction;
    public long batchSize;
    public long outputCount;

    public RowMetaInterface inputRowMeta;
    public RowMetaInterface outputRowMeta;

    public int[] labelStreamIndexes;
    public int[] keyStreamIndexes;
    public int[] fieldStreamIndexes;
    public int dateStreamIndex;

    public int[] columnLookupArray;

    public GraphPropertyType[] keyLookupTypes;
    public GraphPropertyType[] propLookupTypes;

    public List<Integer> lazyList;
    public DimensionGraphMeta.StartDateAlternativeType startDateChoice;
    public int startDateFieldIndex;

    public Date min_date;
    public Date max_date;
    public Date valueDateNow;

    public String dimensionLabelsClause;
    public String returnClause;
    public String dimLookupCypher;
    public String dimInsertCypher;
    public String dimUpdateCypher;
    public String dimPunchCypher;

}
