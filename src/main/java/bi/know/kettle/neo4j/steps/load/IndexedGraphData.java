package bi.know.kettle.neo4j.steps.load;

import bi.know.kettle.neo4j.core.data.GraphData;
import bi.know.kettle.neo4j.core.data.GraphNodeData;
import bi.know.kettle.neo4j.core.data.GraphPropertyData;
import bi.know.kettle.neo4j.core.data.GraphRelationshipData;
import org.pentaho.di.core.exception.KettleException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static bi.know.kettle.neo4j.steps.load.UniquenessStrategy.None;

/**
 * Adds an index to graph data.
 * This allows us to quickly lookup nodes and relationships
 */
public class IndexedGraphData extends GraphData {

  protected UniquenessStrategy nodeUniquenessStrategy;
  protected UniquenessStrategy relationshipUniquenessStrategy;

  protected Map<String, Integer> nodeIdMap;
  protected Map<String, Integer> relIdMap;

  protected List<NodeCollisionListener> nodeCollisionListeners;
  protected List<RelationshipCollisionListener> relCollisionListeners;
  protected Set<IdType> nodePropertiesSet;
  protected Set<IdType> relPropertiesSet;

  public IndexedGraphData() {
    super();

    nodeUniquenessStrategy = None;
    relationshipUniquenessStrategy = None;

    nodeIdMap = new HashMap<>();
    relIdMap = new HashMap<>();

    nodeCollisionListeners = new ArrayList<>();
    relCollisionListeners = new ArrayList<>();

    nodePropertiesSet = new HashSet<>();
    relPropertiesSet = new HashSet<>();
  }

  public IndexedGraphData( UniquenessStrategy nodeUniquenessStrategy, UniquenessStrategy relationshipUniquenessStrategy ) {
    this();
    this.nodeUniquenessStrategy = nodeUniquenessStrategy;
    this.relationshipUniquenessStrategy = relationshipUniquenessStrategy;
  }

  public Integer addAndIndexNode( GraphNodeData node ) throws KettleException {

    Integer index = nodeIdMap.get( node.getId() );
    if ( index == null ) {
      getNodes().add( node );
      nodeIdMap.put( node.getId(), getNodes().size() - 1 );
    } else {

      GraphNodeData existingNode = getNodes().get(index);
      GraphNodeData updatingNode = node;

      switch(nodeUniquenessStrategy) {
        case Last: // Replace with the updating node
          getNodes().set(index, updatingNode);
          break;
        case First: // Simply keep the existing node
        default:
          break;
      }

      // Also call the listeners
      //
      for ( NodeCollisionListener listener : nodeCollisionListeners ) {
        listener.handleCollission( getNodes().get( index ), node );
      }
    }
    for ( GraphPropertyData property : node.getProperties() ) {
      nodePropertiesSet.add( new IdType( property.getId(), property.getType()) );
    }
    return index;
  }

  public Integer addAndIndexRelationship( GraphRelationshipData relationship ) throws KettleException {

    // If we don't have a uniqueness strategy: just add the node...
    //
    Integer index = relIdMap.get( relationship.getId() );
    if ( index == null ) {
      getRelationships().add( relationship );
      relIdMap.put( relationship.getId(), getRelationships().size() - 1 );
    } else {

      GraphRelationshipData existingRelationship = getRelationships().get(index);
      GraphRelationshipData updatingRelationship = relationship;

      switch(relationshipUniquenessStrategy) {
        case Last: // Replace with the updating node
          getRelationships().set(index, updatingRelationship);
          break;
        case First: // Simply keep the existing node
        default:
          break;
      }

      for ( RelationshipCollisionListener listener : relCollisionListeners ) {
        listener.handleCollission( getRelationships().get( index ), relationship );
      }
    }
    for ( GraphPropertyData property : relationship.getProperties() ) {
      relPropertiesSet.add( new IdType( property.getId(), property.getType()) );
    }
    return index;
  }

  public void clearAll() {
    nodes.clear();
    relationships.clear();
    nodeIdMap.clear();
    relIdMap.clear();
  }

  /**
   * Gets nodeIdMap
   *
   * @return value of nodeIdMap
   */
  public Map<String, Integer> getNodeIdMap() {
    return nodeIdMap;
  }

  /**
   * @param nodeIdMap The nodeIdMap to set
   */
  public void setNodeIdMap( Map<String, Integer> nodeIdMap ) {
    this.nodeIdMap = nodeIdMap;
  }

  /**
   * Gets relIdMap
   *
   * @return value of relIdMap
   */
  public Map<String, Integer> getRelIdMap() {
    return relIdMap;
  }

  /**
   * @param relIdMap The relIdMap to set
   */
  public void setRelIdMap( Map<String, Integer> relIdMap ) {
    this.relIdMap = relIdMap;
  }

  /**
   * Gets nodeCollisionListeners
   *
   * @return value of nodeCollisionListeners
   */
  public List<NodeCollisionListener> getNodeCollisionListeners() {
    return nodeCollisionListeners;
  }

  /**
   * @param nodeCollisionListeners The nodeCollisionListeners to set
   */
  public void setNodeCollisionListeners( List<NodeCollisionListener> nodeCollisionListeners ) {
    this.nodeCollisionListeners = nodeCollisionListeners;
  }

  /**
   * Gets relCollisionListeners
   *
   * @return value of relCollisionListeners
   */
  public List<RelationshipCollisionListener> getRelCollisionListeners() {
    return relCollisionListeners;
  }

  /**
   * @param relCollisionListeners The relCollisionListeners to set
   */
  public void setRelCollisionListeners( List<RelationshipCollisionListener> relCollisionListeners ) {
    this.relCollisionListeners = relCollisionListeners;
  }

  /**
   * Gets nodePropertiesSet
   *
   * @return value of nodePropertiesSet
   */
  public Set<IdType> getNodePropertiesSet() {
    return nodePropertiesSet;
  }

  /**
   * @param nodePropertiesSet The nodePropertiesSet to set
   */
  public void setNodePropertiesSet( Set<IdType> nodePropertiesSet ) {
    this.nodePropertiesSet = nodePropertiesSet;
  }

  /**
   * Gets relPropertiesSet
   *
   * @return value of relPropertiesSet
   */
  public Set<IdType> getRelPropertiesSet() {
    return relPropertiesSet;
  }

  /**
   * @param relPropertiesSet The relPropertiesSet to set
   */
  public void setRelPropertiesSet( Set<IdType> relPropertiesSet ) {
    this.relPropertiesSet = relPropertiesSet;
  }
}
