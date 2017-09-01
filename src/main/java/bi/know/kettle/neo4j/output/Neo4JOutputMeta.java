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


@Step(id = "Neo4JOutput", 
image = "NEO4J.svg",
i18nPackageName="bi.know.kettle.neo4j.output",
name="Neo4JOutput.Step.Name",
description = "Neo4JOutput.Step.Description",
categoryDescription="Neo4JOutput.Step.Category",
isSeparateClassLoaderNeeded=true
)
public class Neo4JOutputMeta extends BaseStepMeta implements StepMetaInterface{
	
	public String protocol, host, port, dbName, username, password, key, relationship;  /*label, labelsSeparator*/ 
	public String[] fromNodeProps, fromNodePropNames, toNodeProps, toNodePropNames, fromNodeLabels, toNodeLabels, relProps, relPropNames;
	

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
		protocol = "bolt";
		host = "localhost";
		port = "7687"; 
		dbName = "data"; 
	}
	
	public String getXML() throws KettleException{		
		String retval = "";
		retval += "<protocol>" + protocol + "</protocol>"  + Const.CR;
		retval += "<host>" + host + "</host>"  + Const.CR;
		retval += "<port>" + port + "</port>"  + Const.CR;
		retval += "<username>" + username + "</username>"  + Const.CR;
		retval += "<password>" + password + "</password>"  + Const.CR;
		retval += "<from>" + Const.CR;
		retval += "<labels>" + Const.CR;
		for(int i=0; i < fromNodeLabels.length; i++){
			retval += "    <label>" + fromNodeLabels[i] + "</label>" + Const.CR; 
		}
		retval += "</labels>" + Const.CR;
		retval += "<properties>" + Const.CR;
		for(int i=0; i < fromNodeProps.length; i++){
			retval += "    <property><name>" + fromNodePropNames[i] + "</name><value>" + fromNodeProps[i] + "</value></property>" + Const.CR; 
		}
		retval += "</properties>" + Const.CR;
		retval += "</from>" + Const.CR;
		retval += "<to>" + Const.CR;
		retval += "<labels>" + Const.CR;
		for(int i=0; i < toNodeLabels.length; i++){
			retval += "    <label>" + toNodeLabels[i] + "</label>" + Const.CR; 
		}
		retval += "</labels>" + Const.CR;
		retval += "<properties>" + Const.CR;
		for(int i=0; i < toNodeProps.length; i++){
			retval += "    <property><name>" + toNodePropNames[i] + "</name><value>" + toNodeProps[i] + "</value></property>" + Const.CR; 
		}
		retval += "</properties>" + Const.CR;
		retval += "</to>" + Const.CR;
		
		retval += "<relationship>" + relationship + "</relationship>" + Const.CR;
		
		retval += "<relprops>" + Const.CR;
		for(int i=0; i < relProps.length; i++){
			retval += "    <relprop><name>" + relPropNames[i] + "</name><value>" + relProps[i] + "</value></relprop>" + Const.CR; 
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
		
		Node fromNode = XMLHandler.getSubNode(stepnode, "from");
		Node fromLabelsNode = XMLHandler.getSubNode(fromNode, "labels");
		int nbFromLabelFields =  XMLHandler.countNodes(fromLabelsNode, "label");
		fromNodeLabels= new String[nbFromLabelFields];
		for(int i=0; i < nbFromLabelFields; i++){
			Node labelNode = XMLHandler.getSubNodeByNr(fromLabelsNode, "label", i);
			fromNodeLabels[i] = labelNode.getTextContent();
			logBasic("From Node " + i + ": " + fromNodeLabels[i]);
		}
		Node fromPropsNode = XMLHandler.getSubNode(fromNode, "properties");
		int nbFromPropFields =  XMLHandler.countNodes(fromPropsNode, "property");
		fromNodeProps = new String[nbFromPropFields];
		fromNodePropNames = new String[nbFromPropFields];
		for(int i=0; i < nbFromPropFields; i++){
			Node propNode = XMLHandler.getSubNodeByNr(fromPropsNode, "property", i);
			fromNodeProps[i] = XMLHandler.getSubNode(propNode, "value").getTextContent();
			if(XMLHandler.getSubNode(propNode, "name").getTextContent().isEmpty()) {
				fromNodePropNames[i] = "";
			}else {
				fromNodePropNames[i] = XMLHandler.getSubNode(propNode, "name").getTextContent();
			}
			logBasic("From Node " + i + ": " + fromNodeProps[i] + ", name: " + fromNodePropNames[i]);
		}
		
		Node toNode = XMLHandler.getSubNode(stepnode, "to");
		Node toLabelsNode = XMLHandler.getSubNode(toNode, "labels");
		int nbToLabelFields =  XMLHandler.countNodes(toLabelsNode, "label");
		toNodeLabels= new String[nbToLabelFields];
		for(int i=0; i < nbToLabelFields; i++){
			Node labelNode = XMLHandler.getSubNodeByNr(toLabelsNode, "label", i);
			toNodeLabels[i] = labelNode.getTextContent();
			logBasic("To Node " + i + ": " + toNodeLabels[i]);
		}
		Node toPropsNode = XMLHandler.getSubNode(toNode, "properties");
		int nbToPropFields =  XMLHandler.countNodes(toPropsNode, "property");
		toNodeProps = new String[nbToPropFields];
		toNodePropNames = new String[nbToPropFields];
		for(int i=0; i < nbToPropFields; i++){
			Node propNode = XMLHandler.getSubNodeByNr(toPropsNode, "property", i);
			toNodeProps[i] = XMLHandler.getSubNode(propNode, "value").getTextContent();
			if(XMLHandler.getSubNode(propNode, "name").getTextContent().isEmpty()) {
				toNodePropNames[i] = "";
			}else {
				toNodePropNames[i] = XMLHandler.getSubNode(propNode, "name").getTextContent();
			}
			logBasic("To Node " + i + ": " + toNodeProps[i] +", name: " + toNodePropNames[i]);
		}
		
		
		relationship = XMLHandler.getTagValue(stepnode, "relationship");
		
		Node relPropsNode = XMLHandler.getSubNode(stepnode, "relprops");
		int nbFields =  XMLHandler.countNodes(relPropsNode, "relprop");
		relProps = new String[nbFields];
		relPropNames = new String[nbFields];
		for(int i=0; i < nbFields; i++){
			Node relPropNode = XMLHandler.getSubNodeByNr(relPropsNode, "relprop", i);
			relProps[i] = XMLHandler.getSubNode(relPropNode, "value").getTextContent();
			if(XMLHandler.getSubNode(relPropNode, "name").getTextContent().isEmpty()) {
				relPropNames[i] = "";
			}else {
				relPropNames[i] = XMLHandler.getSubNode(relPropNode, "name").getTextContent();
			}
			logBasic("Relationship Property" + i + ": " + relProps[i] + ", name: " + relPropNames[i]);
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
	
	public void setFromNodeLabels(String[] fromNodeLabels){
		this.fromNodeLabels = fromNodeLabels;
	}
	
	public String[] getFromNodeLabels(){
		return fromNodeLabels;
	}
	
	public void setFromNodeProps(String[] fromNodeProps){
		this.fromNodeProps = fromNodeProps;
	}
	
	public void setFromNodePropNames(String[] fromNodePropNames){
		this.fromNodePropNames = fromNodePropNames;
	}
	
	public String[] getFromNodeProps(){
		return fromNodeProps; 
	}
	
	public String[] getFromNodePropNames(){
		return fromNodePropNames; 
	}
	
	public void setToNodeLabels(String[] toNodeLabels){
		this.toNodeLabels = toNodeLabels;
	}
	
	public String[] getToNodeLabels(){
		return toNodeLabels;
	}
	
	public void setToNodeProps(String[] toNodeProps){
		this.toNodeProps = toNodeProps;
	}
	
	public void setToNodePropNames(String[] toNodePropNames){
		this.toNodePropNames = toNodePropNames;
	}
	
	public String[] getToNodeProps(){
		return toNodeProps; 
	}
	
	public String[] getToNodePropNames(){
		return toNodePropNames; 
	}
	
	public void setRelationship(String relationship){
		this.relationship = relationship; 
	}
	
	public String getRelationship(){
		return relationship; 
	}
	
	public void setRelProps(String[] relProps){
		this.relProps = relProps;
	}
	
	public String[] getRelProps(){
		return relProps; 
	}
	
	public void setRelPropNames(String[] relPropNames) {
		this.relPropNames = relPropNames;
	}
	
	public String[] getRelPropNames() {
		return relPropNames; 
	}	
	
}
