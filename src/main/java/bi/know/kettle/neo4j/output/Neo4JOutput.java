package bi.know.kettle.neo4j.output;

import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


public class Neo4JOutput  extends BaseStep implements StepInterface {
	
	private Neo4JOutputMeta meta; 
	private Neo4JOutputData data;
	private Transaction tx; 
	
    public Neo4JOutput(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s,stepDataInterface,c,t,dis);
	}
    
    
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException{
    	boolean result = true; 
    	
    	meta = (Neo4JOutputMeta)smi;
    	data =  (Neo4JOutputData)sdi;
    	
    	Object[] r = getRow();
    	
        if (first){
            first = false;
            
	          data.outputRowMeta = getInputRowMeta().clone();
	          data.outputRowMeta = (RowMetaInterface)getInputRowMeta().clone();
        }
        
        
        if(r == null){
        	setOutputDone();
        	return false; 
        }
    	
        return result;
    	
    }
    
    
	public boolean init(StepMetaInterface smi, StepDataInterface sdi){
	    meta = (Neo4JOutputMeta)smi;
//	    data = (Neo4JOutputData)sdi;
	    
	    String SERVER_URI = meta.getProtocol() + "://" + meta.getHost() + ":" + meta.getPort() + "/db/" + meta.getDbName();
	    RestAPI graphDb = new RestAPIFacade(SERVER_URI);	    
	    tx = graphDb.beginTx();  
	    
	    
	    
//	    Map<String,Object> props=new HashMap<String, Object>();  
//	    props.put("id",100);  
//	    props.put("name","firstNode");  
//	    Node node=graphDb.createNode(props);  
	    
	    

		return true;
	}
	
	public void dispose(StepMetaInterface smi, StepDataInterface sdi){
	    tx.success();  
	    tx.close(); 
	}



}
