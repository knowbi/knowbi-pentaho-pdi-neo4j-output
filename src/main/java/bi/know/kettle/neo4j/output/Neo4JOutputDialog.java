package bi.know.kettle.neo4j.output;


import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;




public class Neo4JOutputDialog extends BaseStepDialog implements StepDialogInterface{
	private static Class<?> PKG = Neo4JOutputDialog.class; // for i18n purposes, needed by Translator2!!

	
	private Neo4JOutputMeta input; 
	
	private Label wlProtocol, wlHost; 
	private TextVar wProtocol, wHost, wPort, wUsername, wPassword;
	private FormData fdTabFolder, fdlProtocol, fdProtocol, fdlHost, fdHost, fdlPort, fdPort, fdlUsername, fdUsername, fdlPassword, fdPassword, fdFromComp, fdToComp, fdRelationshipsComp, fdGetFromLabel, fdGetFromProps, fdGetToLabel, fdGetToProps, fdRelProps;
	private Combo wRel;
	private TableView 	 wFromPropsGrid, wFromLabelGrid, wToPropsGrid, wToLabelGrid, wRelPropsGrid; 
	private String[]  fieldNames;
	private CTabFolder wTabFolder;
	private CTabItem wFromTab, wToTab, wRelationshipsTab; 
	private Button wGetFromLabel, wGetFromProps, wGetToLabel, wGetToProps, wRelProps;

	
	public Neo4JOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input=(Neo4JOutputMeta)in;
	}


	public String open() {
		Shell parent = getParent();
		Display display = parent.getDisplay();
		
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook( shell );
        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();
		
		// Fields
		try{
			RowMetaInterface prevFields = transMeta.getPrevStepFields( stepname );
			fieldNames = prevFields.getFieldNames();
		}catch(KettleStepException kse){
			logError( BaseMessages.getString( PKG, "TripleOutput.Log.ErrorGettingFieldNames" ));
		}

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Shell.Title")); //$NON-NLS-1$
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.StepName.Label")); //$NON-NLS-1$
        props.setLook( wlStepname );
		fdlStepname=new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right= new FormAttachment(middle, -margin);
		fdlStepname.top  = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
        props.setLook( wStepname );
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// protocol 
		wlProtocol = new Label(shell, SWT.RIGHT);
		wlProtocol.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Protocol.Label"));
		props.setLook(wlProtocol);
		fdlProtocol = new FormData();
		fdlProtocol.left = new FormAttachment(0,0);
		fdlProtocol.right = new FormAttachment(middle,margin);
		fdlProtocol.top = new FormAttachment(0,margin);
		wlProtocol.setLayoutData(fdlProtocol);
		wProtocol = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wProtocol);
		wProtocol.addModifyListener(lsMod);
		fdProtocol = new FormData();
		fdProtocol.left = new FormAttachment(middle, 0);
		fdProtocol.right= new FormAttachment(100, 0);
		fdProtocol.top  = new FormAttachment(wProtocol, margin);
		wProtocol.setLayoutData(fdProtocol);

		// Host line
		wlHost=new Label(shell, SWT.RIGHT);
		wlHost.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Host.Label")); //$NON-NLS-1$
        props.setLook( wlHost );
		fdlHost=new FormData();
		fdlHost.left = new FormAttachment(0, 0);
		fdlHost.right= new FormAttachment(middle, -margin);
		fdlHost.top  = new FormAttachment(wProtocol, margin);
		wlHost.setLayoutData(fdlHost);
		wHost=new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook( wHost );
		wHost.addModifyListener(lsMod);
		fdHost=new FormData();
		fdHost.left = new FormAttachment(middle, 0);
		fdHost.right= new FormAttachment(100, 0);
		fdHost.top  = new FormAttachment(wProtocol, margin);
		wHost.setLayoutData(fdHost);
		
		
		// Port
		Label wlPort = new Label(shell, SWT.RIGHT);
		wlPort.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Port.Label"));
		props.setLook(wlPort);
		fdlPort = new FormData();
		fdlPort.left = new FormAttachment(0,0);
		fdlPort.right = new FormAttachment(middle, -margin);
		fdlPort.top = new FormAttachment(wHost, margin);
		wlPort.setLayoutData(fdlPort);
		wPort = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wPort);
		wPort.addModifyListener(lsMod);
		fdPort = new FormData();
		fdPort.left = new FormAttachment(middle, 0);
		fdPort.right = new FormAttachment(100, 0);
		fdPort.top = new FormAttachment(wHost, margin);
		wPort.setLayoutData(fdPort);
		
		
		// Username
		Label wlUsername = new Label(shell, SWT.RIGHT);
		wlUsername.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Username.Label"));
		props.setLook(wlUsername);
		fdlUsername = new FormData();
		fdlUsername.left = new FormAttachment(0,0);
		fdlUsername.right = new FormAttachment(middle, -margin);
		fdlUsername.top = new FormAttachment(wPort, margin);
		wlUsername.setLayoutData(fdlUsername);
		wUsername = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wUsername);
		wUsername.addModifyListener(lsMod);
		fdUsername = new FormData();
		fdUsername.left = new FormAttachment(middle, 0);
		fdUsername.right = new FormAttachment(100, 0);
		fdUsername.top = new FormAttachment(wPort, margin);
		wUsername.setLayoutData(fdUsername);
		
			
		// Password
		Label wlPassword=new Label(shell, SWT.RIGHT);
		wlPassword.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Password.Label")); //$NON-NLS-1$
        props.setLook( wlPassword);
		fdlPassword = new FormData();
		fdlPassword.left = new FormAttachment(0, 0);
		fdlPassword.right= new FormAttachment(middle, -margin);
		fdlPassword.top  = new FormAttachment(wUsername, margin);
		wlPassword.setLayoutData(fdlPassword);
		Button wbTest =new Button(shell, SWT.PUSH| SWT.CENTER);
        props.setLook( wbTest );
        wbTest.addSelectionListener(new SelectionListener(){

			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			private void processClick(SelectionEvent evt){
			    String SERVER_URI = "bolt://" + wHost.getText() + ":" + wPort.getText();
			    String message = "";
			    try{
				    Driver driver = GraphDatabase.driver(SERVER_URI, AuthTokens.basic(wUsername.getText(), wPassword.getText()));
				    Session session = driver.session();
				    Transaction tx = session.beginTransaction();
				    tx.close();

					message = BaseMessages.getString(PKG, "Neo4JOutputDialog.ConnectionTest.Success");
			    }catch(Exception e){
					message = BaseMessages.getString(PKG, "Neo4JOutputDialog.ConnectionTest.Failed");
			    }
				ShowMessageDialog msgDialog = new ShowMessageDialog(shell, SWT.OK, BaseMessages.getString(PKG, "Neo4JOutputDialog.ConnectionTest.Title") , message);
				msgDialog.open();				
			}
        	
        });
        wbTest.setText(BaseMessages.getString(PKG, "System.Button.Test")); //$NON-NLS-1$
		FormData fdbTest = new FormData();
		fdbTest.right= new FormAttachment(100, 0);
		fdbTest.top  = new FormAttachment(wUsername, margin);
		wbTest.setLayoutData(fdbTest);
		wPassword=new TextVar(transMeta, shell, SWT.PASSWORD | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook( wPassword);
		wPassword.addModifyListener(lsMod);
		fdPassword=new FormData();
		fdPassword.left = new FormAttachment(middle, 0);
		fdPassword.right= new FormAttachment(wbTest, -margin);
		fdPassword.top  = new FormAttachment(wUsername, margin);
		wPassword.setLayoutData(fdPassword);

		
		// Some buttons
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel}, margin, null);
        
		// Add listeners
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();     } };
		
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener    (SWT.Selection, lsOK    );
		
        wTabFolder = new CTabFolder(shell, SWT.BORDER);
 		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
		
		fdTabFolder = new FormData();
		fdTabFolder.left   = new FormAttachment(0, 0);
		fdTabFolder.top    = new FormAttachment(wPassword, margin);
		fdTabFolder.right  = new FormAttachment(100, 0);		
		fdTabFolder.bottom = new FormAttachment(wOK, -margin);
		wTabFolder.setLayoutData(fdTabFolder);
		
		
		/**
		 * FROM
		 */
		wFromTab=new CTabItem(wTabFolder, SWT.NONE);
		wFromTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.FromTab")); //$NON-NLS-1$

		FormLayout fromLayout = new FormLayout ();
		fromLayout.marginWidth  = 3;
		fromLayout.marginHeight = 3;

		Composite wFromComp = new Composite(wTabFolder, SWT.NONE);
 		props.setLook(wFromComp);
		wFromComp.setLayout(fromLayout);		

		// Labels 
		Label wlFromLabel = new Label(wFromComp, SWT.RIGHT);
		wlFromLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.FromLabel"));
		props.setLook(wlFromLabel);
		FormData fdlFromLabels = new FormData();
		fdlFromLabels.left = new FormAttachment(0, 0);
		fdlFromLabels.top = new FormAttachment(wPassword, margin*10);
		wlFromLabel.setLayoutData(fdlFromLabels);
		final int fromLabelRows = input.getFromNodeLabels().length;
		ColumnInfo[] fromLabelInf = new ColumnInfo[]{
			      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.FromLabelsTable.FromFields"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
		};
		wFromLabelGrid =
			    new TableView( Variables.getADefaultVariableSpace(), wFromComp, SWT.BORDER
			      | SWT.FULL_SELECTION | SWT.MULTI, fromLabelInf, fromLabelRows, null, PropsUI.getInstance() );
		props.setLook(wFromLabelGrid);		
		
		wGetFromLabel= new Button(wFromComp, SWT.PUSH);
		wGetFromLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button")); //$NON-NLS-1$
		wGetFromLabel.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			public void processClick(SelectionEvent evt) {
				get(0);
			}
			
		});
		fdGetFromLabel= new FormData();
		fdGetFromLabel.right = new FormAttachment(100, 0);
		fdGetFromLabel.top   = new FormAttachment(wPassword, margin);
		wGetFromLabel.setLayoutData(fdGetFromLabel);
		
		FormData fdFromLabelGrid = new FormData();
		fdFromLabelGrid.left = new FormAttachment(wlFromLabel, 0);
		fdFromLabelGrid.top  = new FormAttachment(wPassword, margin);
		fdFromLabelGrid.right  = new FormAttachment(wGetFromLabel, 0);
		wFromLabelGrid.setLayoutData( fdFromLabelGrid );
		
		
		// Node properties
		Label wlFromFields = new Label(wFromComp, SWT.RIGHT);
		wlFromFields.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFields.Properties"));
		props.setLook(wlFromFields);
		FormData fdlFromFields = new FormData();
		fdlFromFields.left = new FormAttachment(0,0);
		fdlFromFields.top = new FormAttachment(wFromLabelGrid, margin);
		wlFromFields.setLayoutData(fdlFromFields);	
		final int fromPropsRows = input.getFromNodeProps().length;
		ColumnInfo[] colinf =
				    new ColumnInfo[] {
				      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFields"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
				      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFieldsName"), ColumnInfo.COLUMN_TYPE_TEXT, fieldNames )
				      };
		wFromPropsGrid =
				    new TableView( Variables.getADefaultVariableSpace(), wFromComp, SWT.BORDER
				      | SWT.FULL_SELECTION | SWT.MULTI, colinf, fromPropsRows, null, props );
		props.setLook(wFromPropsGrid);
		
		wGetFromProps= new Button(wFromComp, SWT.PUSH);
		wGetFromProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button")); //$NON-NLS-1$
		wGetFromProps.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			private void processClick(SelectionEvent evt) {
				get(1);
			}
		});
		fdGetFromProps= new FormData();
		fdGetFromProps.right = new FormAttachment(100, 0);
		fdGetFromProps.top   = new FormAttachment(wFromLabelGrid, margin);
		wGetFromProps.setLayoutData(fdGetFromProps);
		
		
		FormData fdFromPropsGrid = new FormData();
		fdFromPropsGrid.left = new FormAttachment( wlFromFields, 0 );
		fdFromPropsGrid.right = new FormAttachment(wGetFromProps, 0);
		fdFromPropsGrid.top = new FormAttachment( wFromLabelGrid, margin );
		wFromPropsGrid.setLayoutData( fdFromPropsGrid );

		
		fdFromComp = new FormData();
		fdFromComp.left  = new FormAttachment(0, 0);
		fdFromComp.top   = new FormAttachment(0, 0);
		fdFromComp.right = new FormAttachment(100, 0);
		fdFromComp.bottom= new FormAttachment(100, 0);
		wFromComp.setLayoutData(fdFromComp);

		wFromComp.layout();
		wFromTab.setControl(wFromComp);
		
		
		
		/**
		 * TO
		 */
		
		wToTab=new CTabItem(wTabFolder, SWT.NONE);
		wToTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.ToTab")); //$NON-NLS-1$

		FormLayout toLayout = new FormLayout ();
		toLayout.marginWidth  = 3;
		toLayout.marginHeight = 3;

		Composite wToComp = new Composite(wTabFolder, SWT.NONE);
 		props.setLook(wToComp);
		wToComp.setLayout(toLayout);		

		
		// Labels 
		Label wlToLabel = new Label(wToComp, SWT.RIGHT);
		wlToLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.LabelsField.ToLabel"));
		props.setLook(wlToLabel);
		FormData fdlToLabels = new FormData();
		fdlToLabels.left = new FormAttachment(0, 0);
		fdlToLabels.top = new FormAttachment(wPassword, margin*10);
		wlToLabel.setLayoutData(fdlToLabels);
		final int toLabelRows = input.getToNodeLabels().length;
		ColumnInfo[] toLabelInf = new ColumnInfo[]{
			      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.ToLabelsTable.ToFields"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
		};
		wToLabelGrid =
			    new TableView( Variables.getADefaultVariableSpace(), wToComp, SWT.BORDER
			      | SWT.FULL_SELECTION | SWT.MULTI, toLabelInf, toLabelRows, null, PropsUI.getInstance() );
		props.setLook(wToLabelGrid);
		
		wGetToLabel= new Button(wToComp, SWT.PUSH);
		wGetToLabel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button")); //$NON-NLS-1$
		wGetToLabel.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			public void processClick(SelectionEvent evt) {
				get(2);
			}
			
		});
		fdGetToLabel= new FormData();
		fdGetToLabel.right = new FormAttachment(100, 0);
		fdGetToLabel.top   = new FormAttachment(wPassword, margin);
		wGetToLabel.setLayoutData(fdGetToLabel);

		
		
		FormData fdToLabelGrid = new FormData();
		fdToLabelGrid.left = new FormAttachment( wlToLabel, 0 );
		fdToLabelGrid.right = new FormAttachment(wGetToLabel, 0);
		fdToLabelGrid.top = new FormAttachment( wPassword, margin );
		wToLabelGrid.setLayoutData( fdToLabelGrid );

		
		
		// Node properties
		Label wlToFields = new Label(wToComp, SWT.RIGHT);
		wlToFields.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFields.Properties"));
		props.setLook(wlToFields);
		FormData fdlToFields = new FormData();
		fdlToFields.left = new FormAttachment(0,0);
		fdlToFields.top = new FormAttachment(wToLabelGrid, margin);
		wlToFields.setLayoutData(fdlToFields);
		final int toPropsRows = input.getToNodeProps().length;
		ColumnInfo[] toColinf =
				    new ColumnInfo[] {
				      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFieldsTable.ToFields"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
				      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.ToFieldsTable.ToFieldsName"), ColumnInfo.COLUMN_TYPE_TEXT, false )
				      };

		wToPropsGrid =
				    new TableView( Variables.getADefaultVariableSpace(), wToComp, SWT.BORDER
				      | SWT.FULL_SELECTION | SWT.MULTI, toColinf, toPropsRows, null, PropsUI.getInstance() );

		props.setLook(wToPropsGrid);
		
		
		wGetToProps = new Button(wToComp, SWT.PUSH);
		wGetToProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button")); //$NON-NLS-1$
		wGetToProps.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			public void processClick(SelectionEvent evt) {
				get(3);
			}
			
		});
		fdGetToProps = new FormData();
		fdGetToProps.right = new FormAttachment(100, 0);
		fdGetToProps.top   = new FormAttachment(wToLabelGrid, margin);
		wGetToProps.setLayoutData(fdGetToProps);		
		
		
		FormData fdToPropsGrid = new FormData();
		fdToPropsGrid.left = new FormAttachment( wlToFields, 0 );
		fdToPropsGrid.right = new FormAttachment( wGetToProps, 0);
		fdToPropsGrid.top = new FormAttachment( wToLabelGrid, margin );
		wToPropsGrid.setLayoutData( fdToPropsGrid );
		
		
		fdToComp = new FormData();
		fdToComp.left  = new FormAttachment(0, 0);
		fdToComp.top   = new FormAttachment(0, 0);
		fdToComp.right = new FormAttachment(100, 0);
		fdToComp.bottom= new FormAttachment(100, 0);
		wToComp.setLayoutData(fdToComp);

		wToComp.layout();
		wToTab.setControl(wToComp);

		

		/** 
		 * Relationships
		 */
		wRelationshipsTab=new CTabItem(wTabFolder, SWT.NONE);
		wRelationshipsTab.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.RelationshipsTab")); //$NON-NLS-1$

		FormLayout relationshipsLayout = new FormLayout ();
		relationshipsLayout.marginWidth  = 3;
		relationshipsLayout.marginHeight = 3;

		Composite wRelationshipsComp = new Composite(wTabFolder, SWT.NONE);
 		props.setLook(wRelationshipsComp);
		wRelationshipsComp.setLayout(relationshipsLayout);		

				
		// Relationship field	
		Label wlRel = new Label(wRelationshipsComp, SWT.RIGHT);
		wlRel.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.Relationship.Label"));
		props.setLook(wlRel);
		FormData fdlRel= new FormData();
		fdlRel.left = new FormAttachment(0, 0);
		fdlRel.top = new FormAttachment(wPassword, margin*10);
		wlRel.setLayoutData(fdlRel);
		
		wRel= new Combo(wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wRel.setItems(fieldNames);
		props.setLook(wRel);
		wRel.addModifyListener(lsMod);
		FormData fdRel = new FormData();
		fdRel.left = new FormAttachment(wlRel, 0);
		fdRel.top = new FormAttachment(wPassword, margin*10);
		wRel.setLayoutData(fdRel);
		
		
		// Relationship properties 
		Label wlRelProps= new Label(wRelationshipsComp, SWT.RIGHT);
		wlRelProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.RelationshipField.Label"));
		props.setLook(wlRelProps);
		FormData fdlRelProps = new FormData();
		fdlRelProps.left = new FormAttachment(0, 0);
		fdlRelProps.top = new FormAttachment(wRel, margin*5);
		wlRelProps.setLayoutData(fdlRelProps);
		final int relPropsRows = input.getRelProps().length;
		ColumnInfo[] relPropsInf = new ColumnInfo[]{
			      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesField"), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
			      new ColumnInfo( BaseMessages.getString(PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesFieldName"), ColumnInfo.COLUMN_TYPE_TEXT, false)
		};
		wRelPropsGrid =
			    new TableView( Variables.getADefaultVariableSpace(), wRelationshipsComp, SWT.BORDER
			      | SWT.FULL_SELECTION | SWT.MULTI, relPropsInf, relPropsRows, null, PropsUI.getInstance() );
		props.setLook(wRelPropsGrid);
		
		
		wRelProps = new Button(wRelationshipsComp, SWT.PUSH);
		wRelProps.setText(BaseMessages.getString(PKG, "Neo4JOutputDialog.GetFields.Button")); //$NON-NLS-1$
		wRelProps.addSelectionListener(new SelectionListener() {

			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				processClick(arg0);
				
			}
			
			public void processClick(SelectionEvent evt) {
				get(4);
			}
			
		});
		fdRelProps = new FormData();
		fdRelProps.right = new FormAttachment(100, 0);
		fdRelProps.top   = new FormAttachment(wRel, margin);
		wRelProps.setLayoutData(fdRelProps);		
		
		
		
		FormData fdRelPropsGrid = new FormData();
		fdRelPropsGrid.left = new FormAttachment(wlRelProps, 0 );
		fdRelPropsGrid.right = new FormAttachment(wRelProps, 0);
		fdRelPropsGrid.top = new FormAttachment( wRel, margin *5);
		wRelPropsGrid.setLayoutData( fdRelPropsGrid );		

		
		fdRelationshipsComp = new FormData();
		fdRelationshipsComp.left  = new FormAttachment(0, 0);
		fdRelationshipsComp.top   = new FormAttachment(0, 0);
		fdRelationshipsComp.right = new FormAttachment(100, 0);
		fdRelationshipsComp.bottom= new FormAttachment(100, 0);
		wRelationshipsComp.setLayoutData(fdRelationshipsComp);

		wRelationshipsComp.layout();
		wRelationshipsTab.setControl(wRelationshipsComp);

		
		wTabFolder.setSelection(0);
		
		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
		
		wStepname.addSelectionListener( lsDef );
		
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );

		// Set the shell size, based upon previous time...
		setSize();
		
		getData();
		input.setChanged(changed);
	
		shell.open();
		while (!shell.isDisposed())
		{
		    if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	
	private void getData(){
		wStepname.setText(stepname);
		wStepname.selectAll();
		if(input.getProtocol() != null){
			wProtocol.setText(input.getProtocol());
		}
		
		if(input.getHost() != null){
			wHost.setText(input.getHost());
		}
		
		if(input.getPort() != null){
			wPort.setText(input.getPort());
		}
		
		if(input.getUsername() != null){
			wUsername.setText(input.getUsername());
		}
		
		if(input.getPassword() != null){
			wPassword.setText(input.getPassword());
		}
		
		if(input.getFromNodeLabels() != null){
			String fromNodeLabels[] = input.getFromNodeLabels();
			
			for(int i=0; i < fromNodeLabels.length; i++){
				TableItem item = wFromLabelGrid.table.getItem(i);
				item.setText(1, fromNodeLabels[i]);
			}
		}
		
		
		if(input.getFromNodeProps() != null){
			String fromNodeProps[] = input.getFromNodeProps();
			String fromNodePropNames[] = input.getFromNodePropNames();
			
			for(int i=0; i < fromNodeProps.length; i++){
				TableItem item = wFromPropsGrid.table.getItem(i);
				item.setText(1, fromNodeProps[i]);
				item.setText(2, fromNodePropNames[i]);
			}
		}
		
		
		if(input.getToNodeLabels() != null){
			String toNodeLabels[] = input.getToNodeLabels();
			
			for(int i=0; i < toNodeLabels.length; i++){
				TableItem item = wToLabelGrid.table.getItem(i);
				item.setText(1, toNodeLabels[i]);
			}
		}
		
		
		if(input.getToNodeProps() != null){
			String toNodeProps[] = input.getToNodeProps();
			String toNodePropNames[] = input.getToNodePropNames();
			
			for(int i=0; i < toNodeProps.length; i++){
				TableItem item = wToPropsGrid.table.getItem(i);
				item.setText(1, toNodeProps[i]);
				item.setText(2, toNodePropNames[i]);
			}
		}
		
		if(input.getRelationship() != null){
			wRel.setText(input.getRelationship());
		}
	
		
		if(input.getRelProps() != null){
			String relProps[] = input.getRelProps();
			String relPropNames[] = input.getRelPropNames();
			
			for(int i=0; i < relProps.length; i++){
				TableItem item = wRelPropsGrid.table.getItem(i);
				item.setText(1, relProps[i]);
				item.setText(2, relPropNames[i]);
			}
		}

	}

	private void cancel()
	{
		stepname=null;
		input.setChanged(changed);
		dispose();
	}
	
	private void ok(){

		stepname = wStepname.getText();
		input.setProtocol(wProtocol.getText());
		input.setHost(wHost.getText());
		input.setPort(wPort.getText());
		input.setUsername(wUsername.getText());
		input.setPassword(wPassword.getText());
		
		String fromNodeLabels[] = new String[wFromLabelGrid.nrNonEmpty()];
		for(int i=0; i < fromNodeLabels.length; i++){
			TableItem item = wFromLabelGrid.table.getItem(i);
			fromNodeLabels[i] = item.getText(1);
		}
		input.setFromNodeLabels(fromNodeLabels);
		
		String toNodeLabels[] = new String[wToLabelGrid.nrNonEmpty()];
		for(int i=0; i < toNodeLabels.length; i++){
			TableItem item = wToLabelGrid.table.getItem(i);
			toNodeLabels[i] = item.getText(1);
		}
		input.setToNodeLabels(toNodeLabels);

		int nbFromPropLines = wFromPropsGrid.nrNonEmpty(); 
		String fromNodeProps[] = new String[nbFromPropLines];
		String fromNodePropNames[] = new String[nbFromPropLines];
		for(int i=0; i < fromNodeProps.length; i++){
			TableItem item = wFromPropsGrid.table.getItem(i);
			fromNodeProps[i] = item.getText(1);
			fromNodePropNames[i] = item.getText(2);
		}
		input.setFromNodeProps(fromNodeProps);
		input.setFromNodePropNames(fromNodePropNames);


		int nbToPropLines = wToPropsGrid.nrNonEmpty();
		String toNodeProps[] = new String[nbToPropLines];
		String toNodePropNames[] = new String[nbToPropLines];
		for(int i=0; i < toNodeProps.length; i++){
			TableItem item = wToPropsGrid.table.getItem(i);
			toNodeProps[i] = item.getText(1);
			toNodePropNames[i] = item.getText(2);
		}
		input.setToNodeProps(toNodeProps);
		input.setToNodePropNames(toNodePropNames);
		
		input.setRelationship(wRel.getText());


		int nbRelProps = wRelPropsGrid.nrNonEmpty();
		String relProps[] = new String[nbRelProps];
		String relPropNames[] = new String[nbRelProps]; 
		for(int i=0; i < relProps.length; i++){
			TableItem item = wRelPropsGrid.table.getItem(i);
			relProps[i] = item.getText(1);
			relPropNames[i] = item.getText(2);
		}
		input.setRelProps(relProps);
		input.setRelPropNames(relPropNames);
		
		dispose();
		
	}
	
	
	private void get(int button){
		try{
            RowMetaInterface r = transMeta.getPrevStepFields(stepname);
            if (r!=null && !r.isEmpty()){
    			switch (button){
    			/* 0: from labels grid
    			 * 1: from properties grid
    			 * 2: to labels grid
    			 * 3: to properties grid 
    			 * 4: relationship properties grid
    			 */
    			case 0 :
    				BaseStepDialog.getFieldsFromPrevious(r, wFromLabelGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
    			case 1:
    				BaseStepDialog.getFieldsFromPrevious(r, wFromPropsGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
    			case 2:
    				BaseStepDialog.getFieldsFromPrevious(r, wToLabelGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
    			case 3:
    				BaseStepDialog.getFieldsFromPrevious(r, wToPropsGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
    			case 4:
    				BaseStepDialog.getFieldsFromPrevious(r, wRelPropsGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
    			}
            }
		}
		catch(KettleException ke){
			new ErrorDialog(shell, BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage"), ke); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
}
