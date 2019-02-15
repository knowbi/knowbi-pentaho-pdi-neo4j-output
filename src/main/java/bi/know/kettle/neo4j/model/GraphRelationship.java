package bi.know.kettle.neo4j.model;

import org.json.simple.JSONObject;
import org.pentaho.metastore.persist.MetaStoreAttribute;

import java.util.ArrayList;
import java.util.List;

public class GraphRelationship {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private String label;

  @MetaStoreAttribute
  private List<GraphProperty> properties;

  @MetaStoreAttribute
  private String nodeSource;

  @MetaStoreAttribute
  private String nodeTarget;

  public GraphRelationship() {
    properties = new ArrayList<>();
  }

  public GraphRelationship( String name, String description, String label, List<GraphProperty> properties, String nodeSource, String nodeTarget ) {
    this.name = name;
    this.description = description;
    this.label = label;
    this.properties = properties;
    this.nodeSource = nodeSource;
    this.nodeTarget = nodeTarget;
  }

  @Override public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    if ( !( o instanceof GraphRelationship ) ) {
      return false;
    }
    if ( o == this ) {
      return true;
    }
    return ( (GraphRelationship) o ).getName().equalsIgnoreCase( name );
  }

  @Override public String toString() {
    return name == null ? super.toString() : name;
  }

  public GraphRelationship( GraphRelationship graphRelationship ) {
    this();
    List<GraphProperty> properties = new ArrayList<>();
    for ( GraphProperty property : graphRelationship.getProperties() ) {
      properties.add( new GraphProperty( property.getName(), property.getDescription(), property.getType(), property.isPrimary() ) );
    }

    setName( graphRelationship.getName() );
    setDescription( graphRelationship.getDescription() );
    setLabel( graphRelationship.getLabel() );
    setProperties( properties );
    setNodeSource( graphRelationship.getNodeSource() );
    setNodeTarget( graphRelationship.getNodeTarget() );
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

  public List<GraphProperty> getProperties() {
    return properties;
  }

  public void setProperties( List<GraphProperty> properties ) {
    this.properties = properties;
  }

  public String getNodeSource() {
    return nodeSource;
  }

  public void setNodeSource( String nodeSource ) {
    this.nodeSource = nodeSource;
  }

  public String getNodeTarget() {
    return nodeTarget;
  }

  public void setNodeTarget( String nodeTarget ) {
    this.nodeTarget = nodeTarget;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel( String label ) {
    this.label = label;
  }
}
