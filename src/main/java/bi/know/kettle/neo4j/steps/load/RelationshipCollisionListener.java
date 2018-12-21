package bi.know.kettle.neo4j.steps.load;

import bi.know.kettle.neo4j.core.data.GraphNodeData;
import bi.know.kettle.neo4j.core.data.GraphRelationshipData;
import org.pentaho.di.core.exception.KettleException;

public interface RelationshipCollisionListener {
  /**
   * When a relationship gets added and an existing relationship already exists, let the system know what to do with the existing relationship.
   * This is used to update or aggregate properties in the existing data.
   *
   * @param existing
   * @param added
   *
   * @throws KettleException
   */
  void handleCollission( GraphRelationshipData existing, GraphRelationshipData added ) throws KettleException;
}
