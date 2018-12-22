package bi.know.kettle.neo4j.steps.load;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class LoadData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
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
  public String filesPrefix;
  public String filenameField;
  public String fileTypeField;
}
