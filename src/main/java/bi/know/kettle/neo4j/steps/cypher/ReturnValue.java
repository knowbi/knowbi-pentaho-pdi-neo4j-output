package bi.know.kettle.neo4j.steps.cypher;

public class ReturnValue {
  private String name;
  private String type;

  public ReturnValue() {
  }

  public ReturnValue( String name, String type ) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }
}
