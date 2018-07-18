package bi.know.kettle.neo4j.model;


import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@MetaStoreElementType(
  name = "Neo4j Graph Model",
  description = "Description of the nodes, relationships, indexes... of a Neo4j graph" )
public class GraphModel implements Cloneable {
  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<GraphNode> nodes;

  @MetaStoreAttribute
  private List<GraphRelationship> relationships;

  public GraphModel() {
    nodes = new ArrayList<>();
    relationships = new ArrayList<>();
  }

  public GraphModel( String name, String description, List<GraphNode> nodes, List<GraphRelationship> relationships ) {
    this.name = name;
    this.description = description;
    this.nodes = nodes;
    this.relationships = relationships;
  }

  @Override public boolean equals( Object object ) {
    if ( object == null ) {
      return false;
    }
    if ( !( object instanceof GraphModel ) ) {
      return false;
    }
    if ( object == this ) {
      return true;
    }
    return ( (GraphModel) object ).getName().equalsIgnoreCase( name );
  }

  @Override protected GraphModel clone() {

    GraphModel clone = new GraphModel();
    clone.replace( this );
    return clone;
  }

  /**
   * replace model data
   */
  public void replace( GraphModel source ) {
    setName( source.getName() );
    setDescription( source.getDescription() );

    // Copy nodes
    //
    nodes = new ArrayList<>();
    for ( GraphNode node : source.getNodes() ) {
      nodes.add( node.clone() );
    }

    // replace relationships
    //
    relationships = new ArrayList<>();
    for ( GraphRelationship relationship : source.getRelationships() ) {
      relationships.add( relationship.clone() );
    }
  }

  /**
   * Find a node with the given name, matches case insensitive
   *
   * @param nodeName
   * @return The mode with the given name or null if the node was not found
   */
  public GraphNode findNode( String nodeName ) {
    for ( GraphNode node : nodes ) {
      if ( node.getName().equalsIgnoreCase( nodeName ) ) {
        return node;
      }
    }
    return null;
  }

  /**
   * @return a sorted list of node names
   */
  public String[] getNodeNames() {
    String[] names = new String[ nodes.size() ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = nodes.get( i ).getName();
    }
    Arrays.sort( names );
    return names;
  }


  /**
   * Find a relationship with the given name, matches case insensitive
   *
   * @param relationshipName
   * @return The relationship with the given name or null if the relationship was not found
   */
  public GraphRelationship findRelationship( String relationshipName ) {
    for ( GraphRelationship relationship : relationships ) {
      if ( relationship.getName().equalsIgnoreCase( relationshipName ) ) {
        return relationship;
      }
    }
    return null;
  }

  /**
   * @return a sorted list of relationship names
   */
  public String[] getRelationshipNames() {
    String[] names = new String[ relationships.size() ];
    for ( int i = 0; i < names.length; i++ ) {
      names[ i ] = relationships.get( i ).getName();
    }
    Arrays.sort( names );
    return names;
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

  public List<GraphNode> getNodes() {
    return nodes;
  }

  public void setNodes( List<GraphNode> nodes ) {
    this.nodes = nodes;
  }

  public List<GraphRelationship> getRelationships() {
    return relationships;
  }

  public void setRelationships( List<GraphRelationship> relationships ) {
    this.relationships = relationships;
  }

  /**
   * Find a relationship with source and target, case insensitive
   *
   * @param source
   * @param target
   * @return the relationship or null if nothing was found.
   */
  public GraphRelationship findRelationship( String source, String target ) {
    for ( GraphRelationship relationship : relationships ) {
      if ( relationship.getNodeSource().equalsIgnoreCase( source ) &&
        relationship.getNodeTarget().equalsIgnoreCase( target ) ) {
        return relationship;
      }
    }
    return null;
  }
}
