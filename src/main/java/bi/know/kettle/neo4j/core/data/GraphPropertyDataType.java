package bi.know.kettle.neo4j.core.data;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.types.TypeSystem;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public enum GraphPropertyDataType {
  String("string"),
  Integer("long"),
  Float("double"),
  Number("doubler"),
  Boolean("boolean"),
  Date("date"),
  LocalDateTime("localdatetime"),
  ByteArray(null),
  Time("time"),
  Point(null),
  Duration("duration"),
  LocalTime("localtime"),
  DateTime("datetime");

  private String importType;

  GraphPropertyDataType( java.lang.String importType ) {
    this.importType = importType;
  }

  /**
   * Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */

  public static String getCode( GraphPropertyDataType type ) {
    if ( type == null ) {
      return null;
    }
    return type.name();
  }

  /**
   * Default to String in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static GraphPropertyDataType parseCode( String code ) {
    if ( code == null ) {
      return String;
    }
    try {
      return GraphPropertyDataType.valueOf( code );
    } catch ( IllegalArgumentException e ) {
      return String;
    }
  }

  public static String[] getNames() {
    String[] names = new String[ values().length ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = values()[ i ].name();
    }
    return names;
  }

  public static GraphPropertyDataType getTypeFromNeo4jValue( Object object ) {
    if (object==null) {
      return null;
    }

    if (object instanceof Long) {
      return Integer;
    }
    if (object instanceof Double) {
      return Float;
    }
    if (object instanceof Number) {
      return Number;
    }
    if (object instanceof String) {
      return String;
    }
    if (object instanceof Boolean) {
      return Boolean;
    }
    if (object instanceof LocalDate ) {
      return Date;
    }
    if (object instanceof java.time.LocalDateTime ) {
      return LocalDateTime;
    }
    if (object instanceof java.time.LocalTime ) {
      return LocalTime;
    }
    if (object instanceof java.time.Duration) {
      return Duration;
    }

    throw new RuntimeException( "Unsupported object with class: "+object.getClass().getName() );
  }


  /**
   * Convert the given Kettle value to a Neo4j data type
   *
   * @param valueMeta
   * @param valueData
   * @return
   */
  public Object convertFromKettle( ValueMetaInterface valueMeta, Object valueData ) throws KettleValueException {

    if ( valueMeta.isNull( valueData ) ) {
      return null;
    }
    switch ( this ) {
      case String:
        return valueMeta.getString( valueData );
      case Boolean:
        return valueMeta.getBoolean( valueData );
      case Float:
        return valueMeta.getNumber( valueData );
      case Integer:
        return valueMeta.getInteger( valueData );
      case Date:
        return valueMeta.getDate( valueData ).toInstant().atZone( ZoneId.systemDefault() ).toLocalDate();
      case LocalDateTime:
        return valueMeta.getDate( valueData ).toInstant().atZone( ZoneId.systemDefault() ).toLocalDateTime();
      case ByteArray:
        return valueMeta.getBinary( valueData );
      case Duration:
      case DateTime:
      case Time:
      case Point:
      case LocalTime:
      default:
        throw new KettleValueException(
          "Data conversion to Neo4j type '" + name() + "' from value '" + valueMeta.toStringMeta() + "' is not supported yet" );
    }
  }

  public static final GraphPropertyDataType getTypeFromKettle( ValueMetaInterface valueMeta ) {
    switch ( valueMeta.getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        return GraphPropertyDataType.String;
      case ValueMetaInterface.TYPE_NUMBER:
        return GraphPropertyDataType.Float;
      case ValueMetaInterface.TYPE_DATE:
        return GraphPropertyDataType.LocalDateTime;
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return GraphPropertyDataType.LocalDateTime;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return GraphPropertyDataType.Boolean;
      case ValueMetaInterface.TYPE_BINARY:
        return GraphPropertyDataType.ByteArray;
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return GraphPropertyDataType.String;
      case ValueMetaInterface.TYPE_INTEGER:
        return GraphPropertyDataType.Integer;
      default:
        return GraphPropertyDataType.String;
    }
  }

  /**
   * Gets importType
   *
   * @return value of importType
   */
  public java.lang.String getImportType() {
    return importType;
  }

  /**
   * @param importType The importType to set
   */
  public void setImportType( java.lang.String importType ) {
    this.importType = importType;
  }
}