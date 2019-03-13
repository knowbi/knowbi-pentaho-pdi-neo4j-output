package bi.know.kettle.neo4j.steps.gencsv;

import org.neo4j.kettle.core.data.GraphPropertyDataType;

import java.util.Objects;

public class IdType {

  private String id;
  private GraphPropertyDataType type;

  public IdType() {
  }

  public IdType( String id, GraphPropertyDataType type ) {
    this.id = id;
    this.type = type;
  }

  @Override
  public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    IdType idType = (IdType) o;
    return id.equals( idType.id );
  }

  @Override
  public int hashCode() {
    return Objects.hash( id );
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public GraphPropertyDataType getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( GraphPropertyDataType type ) {
    this.type = type;
  }
}
