package bi.know.kettle.neo4j.core.data;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

public class GraphPropertyData {

  protected String id;

  protected Object value;

  protected GraphPropertyDataType type;

  protected boolean primary;

  public GraphPropertyData() {
  }

  public GraphPropertyData( String id, Object value, GraphPropertyDataType type, boolean primary ) {
    this.id = id;
    this.value = value;
    this.type = type;
    this.primary = primary;
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

    // Replace " with ""
    //
    if ( string.contains( "\"" ) ) {
      string = string.replace( "\"", "\"\"" );
    }

    // Replace \ with \\
    //
    if (string.contains("\\")) {
      string = string.replace( "\\", "\\\\" )
      ;
    }

    return string;
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
    if ( primary ) {
      jProperty.put( "primary", primary);
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
    Object primaryValue = jProperty.get( "primary" );
    primary = primaryValue!=null && ((Boolean)primaryValue);
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

  /**
   * Gets primary
   *
   * @return value of primary
   */
  public boolean isPrimary() {
    return primary;
  }

  /**
   * @param primary The primary to set
   */
  public void setPrimary( boolean primary ) {
    this.primary = primary;
  }
}
