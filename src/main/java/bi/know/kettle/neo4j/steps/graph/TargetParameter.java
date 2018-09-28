package bi.know.kettle.neo4j.steps.graph;

import bi.know.kettle.neo4j.model.GraphPropertyType;

public class TargetParameter {
  private String inputField;
  private int inputFieldIndex;

  private String parameterName;
  private GraphPropertyType parameterType;

  public TargetParameter() {
  }

  public TargetParameter( String inputField, int inputFieldIndex, String parameterName, GraphPropertyType parameterType ) {
    this.inputField = inputField;
    this.inputFieldIndex = inputFieldIndex;
    this.parameterName = parameterName;
    this.parameterType = parameterType;
  }

  /**
   * Gets inputField
   *
   * @return value of inputField
   */
  public String getInputField() {
    return inputField;
  }

  /**
   * @param inputField The inputField to set
   */
  public void setInputField( String inputField ) {
    this.inputField = inputField;
  }

  /**
   * Gets inputFieldIndex
   *
   * @return value of inputFieldIndex
   */
  public int getInputFieldIndex() {
    return inputFieldIndex;
  }

  /**
   * @param inputFieldIndex The inputFieldIndex to set
   */
  public void setInputFieldIndex( int inputFieldIndex ) {
    this.inputFieldIndex = inputFieldIndex;
  }

  /**
   * Gets parameterName
   *
   * @return value of parameterName
   */
  public String getParameterName() {
    return parameterName;
  }

  /**
   * @param parameterName The parameterName to set
   */
  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  /**
   * Gets parameterType
   *
   * @return value of parameterType
   */
  public GraphPropertyType getParameterType() {
    return parameterType;
  }

  /**
   * @param parameterType The parameterType to set
   */
  public void setParameterType( GraphPropertyType parameterType ) {
    this.parameterType = parameterType;
  }
}
