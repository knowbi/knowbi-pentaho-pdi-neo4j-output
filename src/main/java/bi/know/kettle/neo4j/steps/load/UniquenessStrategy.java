package bi.know.kettle.neo4j.steps.load;

public enum UniquenessStrategy {
  None,    // Don't calculate unique nore or relationship values
  First,   // Take the first version of the node or relationship
  Last,    // Take the last version of the node or relationship
  // UpdateProperties, // Not supported yet.  Update all the available properties
  ;

  public static final UniquenessStrategy getStrategyFromName( String name) {
    for (UniquenessStrategy strategy : values()) {
      if (strategy.name().equalsIgnoreCase( name )) {
        return strategy;
      }
    }
    return None;
  }

  public static final String[] getNames() {
    String[] names = new String[values().length];
    for (int i=0;i<names.length;i++) {
      names[i] = values()[i].name();
    }
    return names;
  }
}
