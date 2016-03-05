package bi.know.kettle.neo4j.output.Neo4JOutput;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


@Step( id = "Neo4JOutput",
image = "DPL.svg",
i18nPackageName="bi.know.triple.output",
name="TripleOutput.Step.Name",
description = "TripleOutput.Step.Description",
categoryDescription="TripleOutput.Step.Category"
,isSeparateClassLoaderNeeded=true
)
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface{
	
	public String protocol, host, port, dbName;

	public StepInterface getStep(StepMeta arg0, StepDataInterface arg1, int arg2, TransMeta arg3, Trans arg4) {
		// TODO Auto-generated method stub
		return null;
	}

	public StepDataInterface getStepData() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setDefault() {
		protocol = "http";
		host = "localhost";
		port = "7474"; 
		dbName = "data"; 
		
	}
	
	public void setProtocol(String protocol){
		this.protocol = protocol; 
	}
	
	public String getProtocol(){
		return protocol;
	}
	
	public void setHost(String host){
		this.host = host;
	}
	
	public String getHost(){
		return host; 
	}
	
	public void setPort(String port){
		this.port = port;
	}
	
	public String getPort(){
		return port; 
	}
	
	public void setDbName(String dbName){
		this.dbName = dbName;
	}
	
	public String getDbName(){
		return dbName;
	}
	

}
