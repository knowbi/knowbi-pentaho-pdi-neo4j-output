package bi.know.kettle.neo4j.core.data;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class GraphRelationshipData {

  protected String id;

  protected String label;

  protected List<GraphPropertyData> properties;

  protected String sourceNodeId;

  protected String targetNodeId;

  public GraphRelationshipData() {
    properties = new ArrayList<>();
  }

  public GraphRelationshipData( String id, String label, List<GraphPropertyData> properties, String nodeSource, String nodeTarget ) {
    this.id = id;
    this.label = label;
    this.properties = properties;
    this.sourceNodeId = nodeSource;
    this.targetNodeId = nodeTarget;
  }

  @Override public boolean equals( Object o ) {
    if ( o == null ) {
      return false;
    }
    if ( !( o instanceof GraphRelationshipData ) ) {
      return false;
    }
    if ( o == this ) {
      return true;
    }
    return ( (GraphRelationshipData) o ).getId().equalsIgnoreCase( id );
  }

  @Override public String toString() {
    return id == null ? super.toString() : id;
  }

  public GraphRelationshipData( GraphRelationshipData graphRelationship ) {
    this();

    setId(graphRelationship.getId());
    setLabel( graphRelationship.getLabel() );
    setSourceNodeId( graphRelationship.getSourceNodeId() );
    setTargetNodeId( graphRelationship.getTargetNodeId() );

    List<GraphPropertyData> properties = new ArrayList<>();
    for ( GraphPropertyData property : graphRelationship.getProperties() ) {
      properties.add( new GraphPropertyData( property.getId(), property.getValue(), property.getType(), property.isPrimary() ) );
    }
    setProperties( properties );
  }

  public GraphRelationshipData( Relationship relationship ) {
    this();
    setId( Long.toString( relationship.id() ) );
    setSourceNodeId( Long.toString( relationship.startNodeId() ) );
    setTargetNodeId( Long.toString( relationship.endNodeId() ) );
    setLabel( relationship.type() );
    for ( String propertyKey : relationship.keys() ) {
      Value propertyValue = relationship.get( propertyKey );
      Object propertyObject = propertyValue.asObject();
      GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromNeo4jValue(propertyObject);
      properties.add( new GraphPropertyData( propertyKey, propertyObject, propertyType, false ) );
    }

  }

  public JSONObject toJson() {
    JSONObject jRelationship = new JSONObject();

    jRelationship.put("id", id);
    jRelationship.put("label", label);
    jRelationship.put("sourceNodeId", sourceNodeId );
    jRelationship.put("targetNodeId", targetNodeId );

    if (!properties.isEmpty()) {
      JSONArray jProperties = new JSONArray();
      jRelationship.put( "properties", jProperties );
      for ( GraphPropertyData property : properties ) {
        jProperties.add( property.toJson() );
      }
    }

    return jRelationship;
  }

  public GraphRelationshipData( JSONObject jRelationship ) {
    this();
    id = (String) jRelationship.get("id");
    label = (String) jRelationship.get("label");
    sourceNodeId = (String) jRelationship.get("sourceNodeId");
    targetNodeId = (String) jRelationship.get("targetNodeId");

    JSONArray jProperties = (JSONArray) jRelationship.get("properties");
    if (jProperties!=null) {
      for (int i=0;i<jProperties.size();i++) {
        JSONObject jProperty = (JSONObject) jProperties.get(i);
        properties.add(new GraphPropertyData( jProperty ));
      }
    }
  }


  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * Gets label
   *
   * @return value of label
   */
  public String getLabel() {
    return label;
  }

  /**
   * @param label The label to set
   */
  public void setLabel( String label ) {
    this.label = label;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<GraphPropertyData> getProperties() {
    return properties;
  }

  /**
   * @param properties The properties to set
   */
  public void setProperties( List<GraphPropertyData> properties ) {
    this.properties = properties;
  }

  /**
   * Gets sourceNodeId
   *
   * @return value of sourceNodeId
   */
  public String getSourceNodeId() {
    return sourceNodeId;
  }

  /**
   * @param sourceNodeId The sourceNodeId to set
   */
  public void setSourceNodeId( String sourceNodeId ) {
    this.sourceNodeId = sourceNodeId;
  }

  /**
   * Gets targetNodeId
   *
   * @return value of targetNodeId
   */
  public String getTargetNodeId() {
    return targetNodeId;
  }

  /**
   * @param targetNodeId The targetNodeId to set
   */
  public void setTargetNodeId( String targetNodeId ) {
    this.targetNodeId = targetNodeId;
  }

}
