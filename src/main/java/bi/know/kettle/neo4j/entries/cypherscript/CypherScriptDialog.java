package bi.know.kettle.neo4j.entries.cypherscript;

import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.neo4j.kettle.core.Neo4jDefaults;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.Collections;
import java.util.List;

public class CypherScriptDialog extends JobEntryDialog implements JobEntryDialogInterface {

  public static final String CHECK_CONNECTIONS_DIALOG = "Neo4jCheckConnectionsDialog";
  private static Class<?> PKG = CypherScriptDialog.class; // for i18n purposes, needed by Translator2!!

  private Shell shell;

  private CypherScript jobEntry;

  private boolean changed;

  private Text wName;
  private ComboVar wConnection;
  private TextVar wScript;
  private Button wReplaceVariables;

  private Button wOK, wCancel;

  private String[] availableConnectionNames;
  private MetaStoreFactory<NeoConnection> connectionFactory;

  public CypherScriptDialog( Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntry, rep, jobMeta );
    this.jobEntry = (CypherScript) jobEntry;
    connectionFactory = new MetaStoreFactory<>( NeoConnection.class, Spoon.getInstance().getMetaStore(), Neo4jDefaults.NAMESPACE );

    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( "Neo4j Cypher Script" );
    }
  }

  @Override public JobEntryInterface open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    try {
      List<String> names = connectionFactory.getElementNames();
      Collections.sort( names );
      availableConnectionNames = names.toArray( new String[ 0 ] );
    } catch ( MetaStoreException e ) {
      availableConnectionNames = new String[] {};
    }


    shell.setLayout( formLayout );
    shell.setText( "Neo4j Cypher Script" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Label wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( "Job entry name" );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    Label wlConnection = new Label( shell, SWT.RIGHT );
    wlConnection.setText( "Neo4j Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

    wConnection = new ComboVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    wConnection.setItems( availableConnectionNames );
    lastControl = wConnection;

    // Add buttons first, then the script field can use dynamic sizing
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    Label wlReplaceVariables = new Label( shell, SWT.LEFT );
    wlReplaceVariables.setText( "Replace variables in script?" );
    props.setLook( wlReplaceVariables );
    FormData fdlReplaceVariables = new FormData();
    fdlReplaceVariables.left = new FormAttachment( 0, 0 );
    fdlReplaceVariables.right = new FormAttachment( middle, -margin );
    fdlReplaceVariables.bottom = new FormAttachment( wOK, -margin * 2 );
    wlReplaceVariables.setLayoutData( fdlReplaceVariables );
    wReplaceVariables = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wReplaceVariables );
    FormData fdReplaceVariables = new FormData();
    fdReplaceVariables.left = new FormAttachment( middle, 0 );
    fdReplaceVariables.right = new FormAttachment( 100, 0 );
    fdReplaceVariables.top = new FormAttachment( wlReplaceVariables, 0, SWT.CENTER );
    wReplaceVariables.setLayoutData( fdReplaceVariables );
    lastControl = wReplaceVariables;

    Label wlScript = new Label( shell, SWT.LEFT );
    wlScript.setText( "Cypher Script. Separate commands with ; on a new line." );
    props.setLook( wlScript );
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment( 0, 0 );
    fdlCypher.right = new FormAttachment( 100, 0 );
    fdlCypher.top = new FormAttachment( wConnection, margin );
    wlScript.setLayoutData( fdlCypher );
    wScript = new TextVar( jobMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    wScript.getTextWidget().setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wScript );
    wScript.addModifyListener( lsMod );
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment( 0, 0 );
    fdCypher.right = new FormAttachment( 100, 0 );
    fdCypher.top = new FormAttachment( wlScript, margin );
    fdCypher.bottom = new FormAttachment( wReplaceVariables, -margin * 2 );
    wScript.setLayoutData( fdCypher );
    lastControl = wScript;

    // Put these buttons at the bottom
    //
    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel, }, margin, null );

    // Detect X or ALT-F4 or something that kills this window...
    //
    shell.addListener( SWT.Close, e -> cancel() );
    wName.addListener( SWT.DefaultSelection, e -> ok() );

    wNewConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newConnection();
      }
    } );
    wEditConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        editConnection();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return jobEntry;
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void getData() {
    wName.setText( Const.NVL( jobEntry.getName(), "" ) );
    wConnection.setText( Const.NVL( jobEntry.getConnectionName(), "" ) );
    wScript.setText( Const.NVL( jobEntry.getScript(), "" ) );
    wReplaceVariables.setSelection( jobEntry.isReplacingVariables() );
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( "Warning" );
      mb.setMessage( "The name of the job entry is missing!" );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setConnectionName( wConnection.getText() );
    jobEntry.setScript( wScript.getText() );
    jobEntry.setReplacingVariables( wReplaceVariables.getSelection() );

    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  protected void newConnection() {
    NeoConnection connection = NeoConnectionUtils.newConnection( shell, jobMeta, NeoConnectionUtils.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  protected void editConnection() {
    NeoConnectionUtils.editConnection( shell, jobMeta, NeoConnectionUtils.getConnectionFactory( metaStore ), wConnection.getText() );
  }


}
