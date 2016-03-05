package bi.know.kettle.neo4j.output;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;



@Step( id = "Neo4JOutput",
image = "DPL.svg",
i18nPackageName="bi.know.kettle.neo4j.output",
name="Neo4JOutput.Step.Name",
description = "Neo4JOutput.Step.Description",
categoryDescription="Neo4JOutput.Step.Category"
)
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface{
	
	public String protocol, host, port, dbName;

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
		return new Neo4JOutput(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	public StepDataInterface getStepData() {
		return new Neo4JOutputData();
	}
	
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name){
		return new Neo4JOutputDialog(shell, meta, transMeta, name);
	}


	public void setDefault() {
		protocol = "http";
		host = "localhost";
		port = "7474"; 
		dbName = "data"; 
	}
	
	public String getXML() throws KettleException{		
		String retval = "";
		return retval;
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters) throws KettleXMLException{		
	}
		
	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String,Counter> counters) throws KettleException{
		
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException{
		
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
	{
		CheckResult cr;
		if (prev==null || prev.size()==0)
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta);
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving "+prev.size()+" fields", stepMeta);
			remarks.add(cr);
		}
		
		if (input.length>0)
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta);
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);
			remarks.add(cr);
		}
	}

	public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space){
	}

	public Object clone()
	{
		Object retval = super.clone();
		return retval;
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
