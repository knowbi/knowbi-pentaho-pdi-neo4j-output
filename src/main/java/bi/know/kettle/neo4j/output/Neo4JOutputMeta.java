package bi.know.kettle.neo4j.output;

import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
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
image = "NEO4J.svg",
i18nPackageName="bi.know.kettle.neo4j.output",
name="Neo4JOutput.Step.Name",
description = "Neo4JOutput.Step.Description",
categoryDescription="Neo4JOutput.Step.Category",
isSeparateClassLoaderNeeded=true
)
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface{
	
	public String protocol, host, port, dbName, username, password, key;  /*label, labelsSeparator*/ 
	public String[] nodeProps, nodeLabels, relProps;
	
	public String[][] relationships;

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
		retval += "<protocol>" + protocol + "</protocol>"  + Const.CR;
		retval += "<host>" + host + "</host>"  + Const.CR;
		retval += "<port>" + port + "</port>"  + Const.CR;
		retval += "<username>" + username + "</username>"  + Const.CR;
		retval += "<password>" + password + "</password>"  + Const.CR;
		retval += "<key>" + key + "</key>" + Const.CR;
		retval += "<labels>" + Const.CR;
		for(int i=0; i < nodeLabels.length; i++){
			retval += "    <label>" + nodeLabels[i] + "</label>" + Const.CR; 
		}
		retval += "</labels>" + Const.CR;		
		retval += "<nodes>" + Const.CR;
		for(int i=0; i < nodeProps.length; i++){
			retval += "    <node>" + nodeProps[i] + "</node>" + Const.CR; 
		}
		retval += "</nodes>" + Const.CR;
		retval += "<relationships>" + Const.CR;
		for(int i=0; i < relationships.length ; i++){
			retval += "<relationship>"  + Const.CR; 
			retval += "<fromnode>" +relationships[i][0] + "</fromnode>" + Const.CR; 
			retval += "<relationship>" +relationships[i][1] + "</relationship>" + Const.CR; 
			retval += "<tonode>" +relationships[i][2] + "</tonode>" + Const.CR; 
			retval += "</relationship>"  + Const.CR; 
		}
		retval += "</relationships>" + Const.CR;
		
		retval += "<relprops>" + Const.CR;
		for(int i=0; i < relProps.length; i++){
			retval += "    <relprop>" + relProps[i] + "</relprop>" + Const.CR; 
		}
		retval += "</relprops>" + Const.CR;
		
		return retval;
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters) throws KettleXMLException{		
		protocol = XMLHandler.getTagValue(stepnode, "protocol");
		host = XMLHandler.getTagValue(stepnode, "host");
		port = XMLHandler.getTagValue(stepnode, "port");
		username = XMLHandler.getTagValue(stepnode, "username");
		password = XMLHandler.getTagValue(stepnode, "password");
		key= XMLHandler.getTagValue(stepnode, "key");
		
		Node labelsNode = XMLHandler.getSubNode(stepnode, "labels");
		int nbLabelFields =  XMLHandler.countNodes(labelsNode, "label");
		nodeLabels= new String[nbLabelFields];
		for(int i=0; i < nbLabelFields; i++){
			Node labelNode = XMLHandler.getSubNodeByNr(labelsNode, "label", i);
			nodeLabels[i] = labelNode.getTextContent();
			logBasic("Node " + i + ": " + nodeLabels[i]);
		}
		
		Node relationshipsNode = XMLHandler.getSubNode(stepnode, "relationships");
		int nbRelationships = XMLHandler.countNodes(relationshipsNode, "relationship");
		relationships = new String[nbRelationships][3];
		for(int i=0; i < relationships.length; i++){
			Node relationshipNode = XMLHandler.getSubNodeByNr(relationshipsNode, "relationship", i);
			relationships[i][0] = XMLHandler.getSubNode(relationshipNode, "fromnode").getTextContent(); 
			relationships[i][1] = XMLHandler.getSubNode(relationshipNode, "relationship").getTextContent(); 
			relationships[i][2] = XMLHandler.getSubNode(relationshipNode, "tonode").getTextContent(); 
		}
		
		
		Node nodesNode = XMLHandler.getSubNode(stepnode, "nodes");
		int nbFields =  XMLHandler.countNodes(nodesNode, "node");
		nodeProps = new String[nbFields];
		for(int i=0; i < nbFields; i++){
			Node nodeNode = XMLHandler.getSubNodeByNr(nodesNode, "node", i);
			nodeProps[i] = nodeNode.getTextContent();
			logBasic("Node " + i + ": " + nodeProps[i]);
		}


		Node relPropsNode = XMLHandler.getSubNode(stepnode, "relprops");
		nbFields =  XMLHandler.countNodes(relPropsNode, "relprop");
		relProps = new String[nbFields];
		for(int i=0; i < nbFields; i++){
			Node relPropNode = XMLHandler.getSubNodeByNr(relPropsNode, "relprop", i);
			relProps[i] = relPropNode.getTextContent();
			logBasic("Relationship Property" + i + ": " + relProps[i]);
		}
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
		ValueMetaInterface v = new ValueMetaString();
		v.setName("nodeURI");
		v.setTrimType(ValueMetaString.TRIM_TYPE_BOTH);
		v.setOrigin(origin);
		r.addValueMeta(v);
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
	
	public void setUsername(String username){
		this.username = username;
	}
	
	public String getUsername(){
		return username;
	}
	
	public void setPassword(String password){
		this.password = password;
	}
	
	public String getPassword(){
		return password;
	}
	
	public void setKey(String key){
		this.key = key;
	}
	
	public String getKey(){
		return key;
	}
	
	public void setLabels(String[] nodeLabels){
		this.nodeLabels = nodeLabels;
	}
	
	public String[] getNodeLabels(){
		return nodeLabels;
	}
	
	public void setNodeProps(String[] nodeProps){
		this.nodeProps = nodeProps;
	}
	
	public String[] getNodeProps(){
		return nodeProps; 
	}
	
	public void setRelationships(String[][] relationships){
		this.relationships = relationships; 
	}
	
	public String[][] getRelationships(){
		return relationships; 
	}
	
	public void setRelProps(String[] relProps){
		this.relProps = relProps;
//		System.out.println( relProps.length + "relProps set");
	}
	
	public String[] getRelProps(){
//		System.out.println("Returning " + relProps.length + " relProps");
		return relProps; 
	}
	
}
