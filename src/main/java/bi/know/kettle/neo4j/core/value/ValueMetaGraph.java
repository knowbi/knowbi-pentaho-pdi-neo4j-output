package bi.know.kettle.neo4j.core.value;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaPlugin;
import org.pentaho.di.core.xml.XMLHandler;
import org.w3c.dom.Node;

@ValueMetaPlugin(
  id = "303",
  name = "Graph",
  description = "Graph data type containing nodes, relationships and their properties"
)
public class ValueMetaGraph extends ValueMetaBase implements ValueMetaInterface {

  /**
   * 303 is the number of the room where the movie "The Matrix" starts and where Neo is short by Agent Smith
   */
  public static final int TYPE_GRAPH = 303;

  public ValueMetaGraph() {
    super(null, TYPE_GRAPH);
  }

  public ValueMetaGraph( String name ) {
    super( name, TYPE_GRAPH);
  }

  /**
   * Create the value from an XML node
   *
   * @param node The DOM node to load from
   *
   * @throws KettleException
   */
  public ValueMetaGraph( Node node ) throws KettleException {
    super(node);

  }
}
