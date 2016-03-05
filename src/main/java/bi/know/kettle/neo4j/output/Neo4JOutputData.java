package bi.know.kettle.neo4j.output;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class Neo4JOutputData extends BaseStepData implements StepDataInterface{
	public RowMetaInterface outputRowMeta;
	
	public Neo4JOutputData(){
		super();
	}

}
