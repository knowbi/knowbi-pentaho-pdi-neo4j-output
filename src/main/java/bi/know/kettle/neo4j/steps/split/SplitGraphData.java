package bi.know.kettle.neo4j.steps.split;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.List;

public class SplitGraphData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public int graphFieldIndex;
  public String typeField;
  public String idField;
  public String propertySetField;
}
