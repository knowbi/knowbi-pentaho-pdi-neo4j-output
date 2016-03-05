package bi.know.kettle.neo4j.out.Neo4JOutput;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class Neo4JOutputDialog extends BaseStepDialog implements StepDialogInterface{

	public Neo4JOutputDialog(Shell parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname) {
		super(parent, baseStepMeta, transMeta, stepname);
		// TODO Auto-generated constructor stub
	}

	public String open() {
		// TODO Auto-generated method stub
		return null;
	}

}
