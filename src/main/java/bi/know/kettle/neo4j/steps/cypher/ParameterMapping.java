package bi.know.kettle.neo4j.steps.cypher;

import org.pentaho.di.core.injection.Injection;

public class ParameterMapping {

  @Injection( name = "PARAMETER_NAME", group = "PARAMETERS" )
  private String parameter;

  @Injection( name = "PARAMETER_FIELD", group = "PARAMETERS" )
  private String field;

  @Injection( name = "PARAMETER_NEO4J_TYPE", group = "PARAMETERS" )
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
