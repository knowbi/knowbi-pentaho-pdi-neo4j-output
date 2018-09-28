package bi.know.kettle.neo4j.steps.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class contains a cypher statement and the mapping between each parameter name and the input field
 */
public class CypherParameters {
  private String cypher;
  private List<TargetParameter> targetParameters;

  public CypherParameters() {
    targetParameters = new ArrayList<>();
  }

  public CypherParameters( String cypher, List<TargetParameter> targetParameters ) {
    this.cypher = cypher;
    this.targetParameters = targetParameters;
  }

  /**
   * Gets cypher
   *
   * @return value of cypher
   */
  public String getCypher() {
    return cypher;
  }

  /**
   * @param cypher The cypher to set
   */
  public void setCypher( String cypher ) {
    this.cypher = cypher;
  }

  /**
   * Gets targetParameters
   *
   * @return value of targetParameters
   */
  public List<TargetParameter> getTargetParameters() {
    return targetParameters;
  }

  /**
   * @param targetParameters The targetParameters to set
   */
  public void setTargetParameters( List<TargetParameter> targetParameters ) {
    this.targetParameters = targetParameters;
  }
}
