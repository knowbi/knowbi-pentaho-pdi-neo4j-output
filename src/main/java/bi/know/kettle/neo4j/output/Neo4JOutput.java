package bi.know.kettle.neo4j.output;

import java.net.URI;
import java.util.Arrays;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;



public class Neo4JOutput  extends BaseStep implements StepInterface {
	private static Class<?> PKG = Neo4JOutput.class; // for i18n purposes, needed by Translator2!!

	private Neo4JOutputMeta meta; 
	private Neo4JOutputData data;
	private Transaction tx;
	private int nbRows;
	private String[] fieldNames;
	private Object[] r; 
	private Object[] outputRow;
	private Driver driver; 
	private Session session;
	
    public Neo4JOutput(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s,stepDataInterface,c,t,dis);
	}
    
    
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException{
    	
    	meta = (Neo4JOutputMeta)smi;
    	data =  (Neo4JOutputData)sdi;
    	
    	r = getRow();
    	
        if (first){
            first = false;
            nbRows= 0;
            
	        data.outputRowMeta = (RowMetaInterface)getInputRowMeta().clone();
          	data.outputRowMeta.addValueMeta(new ValueMetaString("nodeURI"));
   	        fieldNames =  data.outputRowMeta.getFieldNames(); 
        }
        
        if(r == null){
        	setOutputDone();
        	return false; 
        }else{
        	try{ 
        		outputRow = RowDataUtil.resizeArray( r, data.outputRowMeta.size());
	   	     	createNode(); 
	   	     	putRow(data.outputRowMeta, outputRow);
	   	     	nbRows++;
	   	     	setLinesWritten(nbRows);
	   	     	setLinesOutput(nbRows);
        	}catch(Exception e){
        		logError(BaseMessages.getString(PKG, "Neo4JOutput.addNodeError") + e.getMessage());
        	}
        	
        	try{
      	     	 // Create relationship 
//        	 	String[][] relationships = meta.getRelationships(); 
//        	 	for(int i=0; i < relationships.length; i++){
//        	 		Object fromFieldVal = r[Arrays.asList(fieldNames).indexOf(relationships[i][0])];
//        	 		String fromPropVal = (String)r[Arrays.asList(fieldNames).indexOf(relationships[i][1])];
//        	 		String relLabelVal = (String)r[Arrays.asList(fieldNames).indexOf(relationships[i][2])];
//        	 		Object toFieldVal = r[Arrays.asList(fieldNames).indexOf(relationships[i][3])];
//        	 		String toPropVal = (String)r[Arrays.asList(fieldNames).indexOf(relationships[i][4])];
//        	 		
//        	 		String fromNodeId = createNode(fromPropVal, fromFieldVal);
//        	 		String toNodeId = createNode(toPropVal, toFieldVal);
//        	 		String relationshipId = createRelationship(fromNodeId, toNodeId, relLabelVal);
//    	       		addRelationshipProperty(relationshipId, meta.getRelProps());
//        	 	}
            	
        	}catch(Exception e){
        		logError(BaseMessages.getString(PKG, "Neo4JOutput.addRelationshipError") + e.getMessage());
        	}
        	return true; 
        }
    }
    
    
	public boolean init(StepMetaInterface smi, StepDataInterface sdi){
	    meta = (Neo4JOutputMeta)smi;
	    data = (Neo4JOutputData)sdi;
	    
	    String url = meta.getProtocol() + "://" + meta.getHost() + ":" + meta.getPort();  
	    driver = GraphDatabase.driver(url, AuthTokens.basic(meta.getUsername(), meta.getPassword()));
	    session = driver.session();
	    tx = session.beginTransaction();
	    
	    return super.init(smi, sdi);
	}

	/**
	 * TODO: 
	 * 1. handle errors on session.close();
	 */
	public void dispose(StepMetaInterface smi, StepDataInterface sdi){
		tx.success();
		tx.close();
		session.close();
		driver.close();
	    super.dispose(smi, sdi);
	}

	
	/**
	 * TODO: 
	 * 1. parameterized statements 
	 * 2. batch mode 
	 * 3. option to return node id (compatible with batch mode?)
	 * @return
	 */
	public int createNode() {
//		String nodeVal = escapeStr(String.valueOf(r[Arrays.asList(fieldNames).indexOf(meta.getKey())]));
		
//		System.out.println("nodeVal: " + nodeVal);
		
		// Add labels
		String labels = "";
		// TODO: convert to List<String>
    	String nodeLabels[] = meta.getFromNodeLabels();
    	for(int i=0; i < nodeLabels.length; i++){
    		String label = escapeStr(String.valueOf(r[Arrays.asList(fieldNames).indexOf(nodeLabels[i])]));
    		labels += label;
    		if(i != (nodeLabels.length)-1) {
    			labels += ":";
    		}
    		System.out.println("label: " + label);
    	}
		
    	// Add properties
    	String props = " { "; 
    	String nodeProps[] = meta.getFromNodeProps();
    	for(int i=0; i < nodeProps.length; i++){
    		props += nodeProps[i] + " : " + "\"" + String.valueOf(r[Arrays.asList(fieldNames).indexOf(nodeProps[i])]) + "\"";
    		if(i != (nodeProps.length)-1) {
    			props += ", ";
    		}
    		// e.g. { name: 'Andres', title: 'Developer' }
    	}
    	props += "}";

		// CREATE (n:Person:Mens:`Human Being` { name: 'Andres', title: 'Developer' }) return n;
//    	String stmt = "CREATE (" + nodeVal + ":" + labels + props + ");";
    	String stmt = "CREATE (" + labels + props + ");";
    	
    	System.out.println(stmt);
    	
    	
    	try{
    		tx.run(stmt);
    	}catch(Exception e) {
        	logError("Error executing statement: " + stmt);
    	}
		return 0; 
	}
	
	
	public String escapeStr(String str) {
		if(str.contains(" ") || str.contains(".")) {
			str = "`" + str + "`";
		}
		return str; 
	}
    
//    public String createNode(String nodeKey, Object nodeValue){
//    		String nodeJSON = ""; 
//    		if(nodeValue instanceof String){
//    	    	nodeJSON = "{\"key\": \"" + nodeKey + "\", \"value\" : \"" + nodeValue  +   "\"}";    
//    		}else{
//    	    	nodeJSON = "{\"key\": \"" + nodeKey + "\", \"value\" : " + nodeValue  +   "}";    
//    		}
//    	System.out.println("nodeJSON: " + nodeJSON);
//    	Client c = Client.create();
//    	c.addFilter(new HTTPBasicAuthFilter(meta.getUsername(), meta.getPassword()));
//	    WebResource nodeResource = c.resource(SERVER_URI + "index/node/pdi?uniqueness=get_or_create");    	
//    	ClientResponse nodeResponse = nodeResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(nodeJSON).post(ClientResponse.class);
//    	final URI nodeLocation = nodeResponse.getLocation();
//    	outputRow[data.outputRowMeta.size()-1] = nodeLocation.toString();
//    	if(nodeResponse.getStatus() == 201 || nodeResponse.getStatus() == 200){
//    		logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addNodeLabel") + nodeLocation);
//        	return getIdFromURI(nodeLocation);
//    	}else{
//    		// throw exception.
//    		logError(BaseMessages.getString("Neo4JOutput.addNodeError"));
//    		return ""; 
//    	}
//    }
    
    
    
    
//    private int addNodeToLabel(String nodeId, String labelStr){
//    	try{
//        	URI labelURI = new URI(SERVER_URI + "node/" + nodeId + "/labels");
//        	Client labelClient = Client.create();
//        	labelClient.addFilter(new HTTPBasicAuthFilter(meta.getUsername(), meta.getPassword()));
//    	    WebResource labelResource = labelClient.resource(labelURI);
//    		String labelJSON = "\"" + labelStr + "\"";
//        	ClientResponse labelResponse = labelResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(labelJSON).post(ClientResponse.class);
//        	final URI labelLocation = labelResponse.getLocation();
//        	logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addNodeLabel") + labelLocation);
//        	return 0; 
//    	}catch(Exception e){
//    		logError(BaseMessages.getString(PKG, "Neo4JOutput.addNodeLabelError") + nodeId);
//    		return -1; 
//    	}
//    }
    
    
//    private int addNodeProperty(String nodeId, String propKey, String propValue){
//    	try{
//        	URI propertyURI = new URI(SERVER_URI + "node/" + nodeId + "/properties/" + propKey);
//        	Client propertyClient = Client.create();
//        	propertyClient.addFilter(new HTTPBasicAuthFilter(meta.getUsername(), meta.getPassword()));
//    	    WebResource propertyResource = propertyClient.resource(propertyURI);
//    		String propertyJSON = "\"" + propValue + "\"";
//        	ClientResponse propertyResponse = propertyResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(propertyJSON).put(ClientResponse.class);
//        	final URI propertyLocation = propertyResponse.getLocation();
//        	logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addNodeProperty") + propertyLocation);
//        	return 0; 
//    	}catch(Exception e ){
//        	logError(BaseMessages.getString(PKG, "Neo4JOutput.addNodePropertyError") + nodeId);
//        	return -1; 
//    	}
//    }
    
    private String createRelationship(String fromNodeId, String toNodeId, String relationshipType){
    	logDetailed("creating relationship from " + fromNodeId + " to " + toNodeId + " , type: " + relationshipType); 
    	return "";
//    	try{
//     		URI fromNodeURI = new URI(SERVER_URI + "node/" + fromNodeId + "/relationships");
//           	URI toNodeURI = new URI(SERVER_URI + "node/" + toNodeId);
//           	String  relationshipJSON = "{ \"to\" : \"" + toNodeURI + "\", \"type\" : \"" +  relationshipType + "\"}" ;
//           	System.out.println("fromNode: " + fromNodeURI);
//           	System.out.println("relationshipJSON: " + relationshipJSON);
//           	Client fromNodeClient = Client.create(); 
//           	fromNodeClient.addFilter(new HTTPBasicAuthFilter(meta.getUsername(), meta.getPassword()));
//           	WebResource fromNodeResource = fromNodeClient.resource(fromNodeURI);
//           	ClientResponse relationshipResponse = fromNodeResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relationshipJSON).post(ClientResponse.class);
//           	final URI relationshipURI = relationshipResponse.getLocation();
//           	System.out.println("relationshipURI: " + relationshipURI);
//           	logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addRelationship") + fromNodeId + " - " + toNodeId + " - " + relationshipType + " - " + relationshipURI + " - " + relationshipResponse.getStatus() + " - " + relationshipJSON);
//   	     	nbRows++;
//   	     	setLinesWritten(nbRows);
//   	     	setLinesOutput(nbRows);
//
//           	return getIdFromURI(relationshipURI);
//    	}catch(Exception e){
//        	logError(BaseMessages.getString(PKG, "Neo4JOutput.addRelationshipError") + fromNodeId + ", "  + toNodeId  + ", " + relationshipType);
//        	return ""; 
//    	}
    }
    
    
      private int addRelationshipProperty(String relationshipId, String[] relProps){
 		System.out.println("######## creating relationship: " + relationshipId + ", " + Arrays.toString(relProps));

    	try{
    		String relPropsJSON = "{";
    		for(int i=0; i < relProps.length; i++){
    			System.out.println("%%%%%% In loop");
    			String propName = (String)relProps[i];
//    			System.out.println("propName: " + propName);
//    			System.out.println("propVal parts: " + Arrays.toString(fieldNames) + " -- " + (String)relProps[i] + " -- " + Arrays.asList(fieldNames).indexOf(relProps[i]));
//    			System.out.println("Distance: " + Arrays.toString(r));
//    			System.out.println("val: " + r[Arrays.asList(fieldNames).indexOf(relProps[i])]);
    			String propVal = (String)r[Arrays.asList(fieldNames).indexOf(relProps[i])];
//    			System.out.println("propVal: " + propVal);
//    			System.out.println("Name: " + propName + ", val: " + propVal);
    			relPropsJSON += "\"" + propName + "\" : \"" +  propVal + "\" ";
    			if(i <  (relProps.length-1)){
    				relPropsJSON += ", ";
    			}
    		}
//    		relPropsJSON += "}" ;
//    		System.out.println("#############################");
//    		System.out.println("relPropsJSON: " + relPropsJSON);
//    		System.out.println("#############################");
//	       	Client relPropsClient = Client.create(); 
//        	relPropsClient.addFilter(new HTTPBasicAuthFilter(meta.getUsername(), meta.getPassword()));
//        	String relationshipURI = SERVER_URI +  "relationship/" + relationshipId + "/properties"; 
//	       	WebResource relPropsResource = relPropsClient.resource(relationshipURI);
//	       	ClientResponse relPropsResponse = relPropsResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relPropsJSON).put(ClientResponse.class);
//	       	final URI relPropsURI = relPropsResponse.getLocation();
//        	logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addRelationshipProperty") + relPropsURI);
	       	return 0; 
    	}catch(Exception e){
    		System.out.println("ERROR adding relationship properties: " + e.getMessage());
        	logDetailed(BaseMessages.getString(PKG, "Neo4JOutput.addRelationshipPropertyError") + relationshipId);
        	return -1; 
    	}
    }
    
    private String getIdFromURI(URI uri){
    	return uri.toString().substring(uri.toString().lastIndexOf("/")+1);
    }

    
}
