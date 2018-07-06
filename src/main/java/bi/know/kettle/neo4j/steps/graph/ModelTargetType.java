package bi.know.kettle.neo4j.steps.graph;

public enum ModelTargetType {
  Unmapped,
  Node,
  Relationship,
  ;

  /** Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */

    public static String getCode( ModelTargetType type) {
      if (type==null) {
        return null;
      }
      return type.name();
    }

  /**
   * Default to Unmapped in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static ModelTargetType parseCode( String code) {
      if (code==null) {
        return Unmapped;
      }
      try {
        return ModelTargetType.valueOf( code );
      } catch(IllegalArgumentException e) {
        return Unmapped;
      }
    }

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }
}