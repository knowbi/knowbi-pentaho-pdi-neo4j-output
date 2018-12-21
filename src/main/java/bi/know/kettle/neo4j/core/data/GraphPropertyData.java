package bi.know.kettle.neo4j.core.data;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

public class GraphPropertyData {

  protected String id;

  protected Object value;

  protected GraphPropertyDataType type;

  public GraphPropertyData() {
  }

  public GraphPropertyData( String id, Object value, GraphPropertyDataType type ) {
    this.id = id;
    this.value = value;
    this.type = type;
  }

  @Override public String toString() {
    if ( value == null ) {
      return "";
    }
    switch ( type ) {
      case Boolean:
        return ( (Boolean) value ) ? "true" : "false";
      case String:
        return escapeString( (String) value );
      default:
        return value.toString();
    }
  }

  public static String escapeString( String string ) {
    if ( string.contains( "\"" ) ) {
      return string.replace( "\"", "\"\"" );
    } else {
      return string;
    }
  }

  public Object toJson() {
    JSONObject jProperty = new JSONObject();

    jProperty.put( "id", id );
    if ( type != null ) {
      jProperty.put( "type", type.name() );
    }
    if ( value != null ) {
      jProperty.put( "value", value );
    }

    return jProperty;
  }

  public GraphPropertyData( JSONObject jProperty ) {
    this();

    id = (String) jProperty.get( "id" );

    String typeCode = (String) jProperty.get( "type" );
    if ( StringUtils.isNotEmpty( typeCode ) ) {
      type = GraphPropertyDataType.parseCode( typeCode );
    }
    value = jProperty.get( "value" );
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
   * Gets value
   *
   * @return value of value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @param value The value to set
   */
  public void setValue( Object value ) {
    this.value = value;
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
