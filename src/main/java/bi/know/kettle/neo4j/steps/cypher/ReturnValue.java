package bi.know.kettle.neo4j.steps.cypher;

import org.pentaho.di.core.injection.Injection;

public class ReturnValue {

  @Injection( name = "RETURN_NAME", group = "RETURNS" )
  private String name;

  @Injection( name = "RETURN_TYPE", group = "RETURNS" )
  private String type;

  @Injection (name = "RETURN_SOURCE_TYPE", group = "RETURNS" )
  private String sourceType;

  public ReturnValue() {
  }

  public ReturnValue( String name, String type, String sourceType ) {
    this.name = name;
    this.type = type;
    this.sourceType = sourceType;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( String type ) {
    this.type = type;
  }

  /**
   * Gets sourceType
   *
   * @return value of sourceType
   */
  public String getSourceType() {
    return sourceType;
  }

  /**
   * @param sourceType The sourceType to set
   */
  public void setSourceType( String sourceType ) {
    this.sourceType = sourceType;
  }
}
