package bi.know.kettle.neo4j.steps;

import bi.know.kettle.neo4j.core.GraphUsage;
import org.pentaho.di.trans.step.BaseStepData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BaseNeoStepData extends BaseStepData {

  /**
   * A map : GraphUsage / StepName / NodeLabels
   */

  public Map<String, Map<String, Set<String>>> usageMap = new HashMap<>();
}
