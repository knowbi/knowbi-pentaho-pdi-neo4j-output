package bi.know.kettle.neo4j.steps.graph;

public class FieldModelMapping {

  /**
   * The Kettle input field where the data is coming from
   */
  private String field;

  /**
   * Write to a node or a relationship
   */
  private ModelTargetType targetType;

  /**
   * Name of the node or relationship to write to
   */
  private String targetName;

  /**
   * Name of the property to write to
   */
  private String targetProperty;


  public FieldModelMapping() {
    targetType = ModelTargetType.Node;
  }

  public FieldModelMapping( String field, ModelTargetType targetType, String targetName, String targetProperty ) {
    this.field = field;
    this.targetType = targetType;
    this.targetName = targetName;
    this.targetProperty = targetProperty;
  }

  public String getField() {
    return field;
  }

  public void setField( String field ) {
    this.field = field;
  }

  public ModelTargetType getTargetType() {
    return targetType;
  }

  public void setTargetType( ModelTargetType targetType ) {
    this.targetType = targetType;
  }

  public String getTargetName() {
    return targetName;
  }

  public void setTargetName( String targetName ) {
    this.targetName = targetName;
  }

  public String getTargetProperty() {
    return targetProperty;
  }

  public void setTargetProperty( String targetProperty ) {
    this.targetProperty = targetProperty;
  }
}
