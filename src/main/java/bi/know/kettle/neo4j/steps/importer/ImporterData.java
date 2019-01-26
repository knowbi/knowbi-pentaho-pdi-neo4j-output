package bi.know.kettle.neo4j.steps.importer;

import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.List;

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
  public String readBufferSize;
  public String maxMemory;
}
