package bi.know.kettle.neo4j.steps.cypher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CypherStatement {

  private Object[] row;
  private String cypher;
  private Map<String, Object> parameters;
  private List<Object[]> resultRows;

  public CypherStatement() {
    parameters = new HashMap<>();
  }

  public CypherStatement( Object[] row, String cypher, Map<String, Object> parameters ) {
    this.row = row;
    this.cypher = cypher;
    this.parameters = parameters;
  }

  /**
   * Gets row
   *
   * @return value of row
   */
  public Object[] getRow() {
    return row;
  }

  /**
   * @param row The row to set
   */
  public void setRow( Object[] row ) {
    this.row = row;
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
   * Gets parameters
   *
   * @return value of parameters
   */
  public Map<String, Object> getParameters() {
    return parameters;
  }

  /**
   * @param parameters The parameters to set
   */
  public void setParameters( Map<String, Object> parameters ) {
    this.parameters = parameters;
  }

  /**
   * Gets resultRows
   *
   * @return value of resultRows
   */
  public List<Object[]> getResultRows() {
    return resultRows;
  }

  /**
   * @param resultRows The resultRows to set
   */
  public void setResultRows( List<Object[]> resultRows ) {
    this.resultRows = resultRows;
  }
}
