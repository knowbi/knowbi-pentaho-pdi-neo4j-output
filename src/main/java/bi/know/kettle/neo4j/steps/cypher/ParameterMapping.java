package bi.know.kettle.neo4j.steps.cypher;

public class ParameterMapping {

  private String parameter;
  private String field;
  private String neoType;

  public ParameterMapping() {
  }

  public ParameterMapping( String parameter, String field, String neoType ) {
    this.parameter = parameter;
    this.field = field;
    this.neoType = neoType;
  }

  public String getParameter() {
    return parameter;
  }

  public void setParameter( String parameter ) {
    this.parameter = parameter;
  }

  public String getField() {
    return field;
  }

  public void setField( String field ) {
    this.field = field;
  }

  public String getNeoType() {
    return neoType;
  }

  public void setNeoType( String neoType ) {
    this.neoType = neoType;
  }


}
