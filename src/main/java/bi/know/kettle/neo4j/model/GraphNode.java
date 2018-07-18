package bi.know.kettle.neo4j.model;

import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.List;

public class GraphNode implements Cloneable {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<String> labels;

  @MetaStoreAttribute
  private List<GraphProperty> properties;

  public GraphNode() {
    labels = new ArrayList<>();
    properties = new ArrayList<>();
  }

  public GraphNode( String name, String description, List<String> labels, List<GraphProperty> properties ) {
    this.name = name;
    this.description = description;
    this.labels = labels;
    this.properties = properties;
  }

  @Override protected GraphNode clone() {

    GraphNode node = new GraphNode();
    node.replace( this );
    return node;
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

  private void replace( GraphNode graphNode ) {
    setName( graphNode.getName() );
    setDescription( graphNode.getDescription() );

    // Copy labels
    setLabels( new ArrayList<>( graphNode.getLabels() ) );

    // Copy properties
    List<GraphProperty> propertiesCopy = new ArrayList<>();
    for ( GraphProperty propertyCopy : graphNode.getProperties() ) {
      propertiesCopy
        .add( new GraphProperty( propertyCopy.getName(), propertyCopy.getDescription(), propertyCopy.getType(), propertyCopy.isPrimary() ) );
    }
    setProperties( propertiesCopy );
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }

  public List<String> getLabels() {
    return labels;
  }

  public void setLabels( List<String> labels ) {
    this.labels = labels;
  }

  public List<GraphProperty> getProperties() {
    return properties;
  }

  public void setProperties( List<GraphProperty> properties ) {
    this.properties = properties;
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
}
