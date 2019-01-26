package bi.know.kettle.neo4j.steps.gencsv;

import bi.know.kettle.neo4j.core.data.GraphNodeData;
import org.pentaho.di.core.exception.KettleException;

public interface NodeCollisionListener {
  /**
   * When a node gets added and an existing node already exists, let the system know what to do with the existing node.
   * This is used to update or aggregate properties in the existing data.
   *
   * @param existing
   * @param added
   *
   * @throws KettleException
   */
  void handleCollission( GraphNodeData existing, GraphNodeData added) throws KettleException;
}
