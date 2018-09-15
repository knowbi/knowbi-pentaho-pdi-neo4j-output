package bi.know.kettle.neo4j.model;

import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.List;

public class GraphNode {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<String> labels;

  @MetaStoreAttribute
  private List<GraphProperty> properties;

  @MetaStoreAttribute
  private GraphPresentation presentation;

  public GraphNode() {
    labels = new ArrayList<>();
    properties = new ArrayList<>();
    presentation = new GraphPresentation( 0,0 );
  }

  public GraphNode( String name, String description, List<String> labels, List<GraphProperty> properties ) {
    this.name = name;
    this.description = description;
    this.labels = labels;
    this.properties = properties;
    this.presentation = new GraphPresentation( 0,0 );
  }

  public GraphNode( GraphNode graphNode ) {
    this();
    setName( graphNode.getName() );
    setDescription( graphNode.getDescription() );

    // Copy labels
    setLabels( new ArrayList<>( graphNode.getLabels() ) );

    // Copy properties
    List<GraphProperty> propertiesCopy = new ArrayList<>();
    for ( GraphProperty property : graphNode.getProperties() ) {
      GraphProperty propertyCopy = new GraphProperty( property.getName(), property.getDescription(), property.getType(), property.isPrimary() );
      propertiesCopy.add( propertyCopy );
    }
    setProperties( propertiesCopy );
    setPresentation( graphNode.getPresentation().clone() );
  }

  @Override public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    if ( !( o instanceof GraphNode ) ) {
      return false;
    }
    if ( o == this ) {
      return true;
    }
    return ( (GraphNode) o ).getName().equalsIgnoreCase( name );
  }

  /**
   * Search for the property with the given name, case insensitive
   *
   * @param name the name of the property to look for
   * @return the property or null if nothing could be found.
   */
  public GraphProperty findProperty( String name ) {
    for ( GraphProperty property : properties ) {
      if ( property.getName().equalsIgnoreCase( name ) ) {
        return property;
      }
    }
    return null;
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
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets labels
   *
   * @return value of labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * @param labels The labels to set
   */
  public void setLabels( List<String> labels ) {
    this.labels = labels;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<GraphProperty> getProperties() {
    return properties;
  }

  /**
   * @param properties The properties to set
   */
  public void setProperties( List<GraphProperty> properties ) {
    this.properties = properties;
  }

  /**
   * Gets presentation
   *
   * @return value of presentation
   */
  public GraphPresentation getPresentation() {
    return presentation;
  }

  /**
   * @param presentation The presentation to set
   */
  public void setPresentation( GraphPresentation presentation ) {
    this.presentation = presentation;
  }
}
