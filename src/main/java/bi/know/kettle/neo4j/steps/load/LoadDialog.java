package bi.know.kettle.neo4j.steps.load;

import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.Collections;
import java.util.List;

public class LoadDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = LoadMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;

  private CCombo wConnection;
  private CCombo wGraphField;
  private TextVar wDatabaseFilename;
  private TextVar wAdminCommand;
  private TextVar wBaseFolder;  
  private CCombo wStrategy;


  private LoadMeta input;

  public LoadDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (LoadMeta) inputMetadata;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout shellLayout = new FormLayout();
    shell.setLayout( shellLayout );
    shell.setText( "Neo4j Load" );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL| SWT.H_SCROLL );
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout( scFormLayout );
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment( 0, 0 );
    fdSComposite.right = new FormAttachment( 100, 0 );
    fdSComposite.top = new FormAttachment( 0, 0 );
    fdSComposite.bottom = new FormAttachment( 100, 0 );
    wScrolledComposite.setLayoutData( fdSComposite );

    Composite wComposite = new Composite( wScrolledComposite, SWT.NONE );
    props.setLook( wComposite );
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( 0, 0 );
    fdComposite.right =  new FormAttachment( 100, 0 );
    fdComposite.top =  new FormAttachment( 0, 0 );
    fdComposite.bottom =  new FormAttachment( 100, 0 );
    wComposite.setLayoutData( fdComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( formLayout );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( wComposite, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    // Connection
    //
    Label wlConnection = new Label( wComposite, SWT.RIGHT );
    wlConnection.setText( "Neo4j Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

    wConnection = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;

    String[] fieldnames = new String[] {};
    try {
      fieldnames = transMeta.getPrevStepFields(stepMeta).getFieldNames();
    } catch ( KettleStepException e ) {
      log.logError("error getting input field names: ", e);
    }

    wlConnection.setEnabled( false );
    wConnection.setEnabled( false );
    wNewConnection.setEnabled( false );
    wEditConnection.setEnabled( false );

    // Graph field
    //
    Label wlGraphField = new Label( wComposite, SWT.RIGHT );
    wlGraphField.setText( "Graph field " );
    props.setLook( wlGraphField );
    FormData fdlGraphField = new FormData();
    fdlGraphField.left = new FormAttachment( 0, 0 );
    fdlGraphField.right = new FormAttachment( middle, -margin );
    fdlGraphField.top = new FormAttachment( lastControl, 2 * margin );
    wlGraphField.setLayoutData( fdlGraphField );
    wGraphField = new CCombo( wComposite, SWT.CHECK | SWT.BORDER );
    wGraphField.setItems( fieldnames );
    props.setLook( wGraphField );
    FormData fdGraphField = new FormData();
    fdGraphField.left = new FormAttachment( middle, 0 );
    fdGraphField.right = new FormAttachment( 100, 0 );
    fdGraphField.top = new FormAttachment( wlGraphField, 0, SWT.CENTER );
    wGraphField.setLayoutData( fdGraphField );
    lastControl = wGraphField;

    // The database filename to load to
    //
    Label wlDatabaseFilename = new Label( wComposite, SWT.RIGHT );
    wlDatabaseFilename.setText( "Database filename " );
    props.setLook( wlDatabaseFilename );
    FormData fdlDatabaseFilename = new FormData();
    fdlDatabaseFilename.left = new FormAttachment( 0, 0 );
    fdlDatabaseFilename.right = new FormAttachment( middle, -margin );
    fdlDatabaseFilename.top = new FormAttachment( lastControl, 2 * margin );
    wlDatabaseFilename.setLayoutData( fdlDatabaseFilename );
    wDatabaseFilename = new TextVar(transMeta, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatabaseFilename );
    wDatabaseFilename.addModifyListener( lsMod );
    FormData fdDatabaseFilename = new FormData();
    fdDatabaseFilename.left = new FormAttachment( middle, 0 );
    fdDatabaseFilename.right = new FormAttachment( 100, 0 );
    fdDatabaseFilename.top = new FormAttachment( wlDatabaseFilename, 0, SWT.CENTER );
    wDatabaseFilename.setLayoutData( fdDatabaseFilename );
    lastControl = wDatabaseFilename;

    // The path to the neo4j-admin command to use
    //
    Label wlAdminCommand = new Label( wComposite, SWT.RIGHT );
    wlAdminCommand.setText( "neo4j-admin command path " );
    props.setLook( wlAdminCommand );
    FormData fdlAdminCommand = new FormData();
    fdlAdminCommand.left = new FormAttachment( 0, 0 );
    fdlAdminCommand.right = new FormAttachment( middle, -margin );
    fdlAdminCommand.top = new FormAttachment( lastControl, 2 * margin );
    wlAdminCommand.setLayoutData( fdlAdminCommand );
    wAdminCommand = new TextVar(transMeta, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAdminCommand );
    wAdminCommand.addModifyListener( lsMod );
    FormData fdAdminCommand = new FormData();
    fdAdminCommand.left = new FormAttachment( middle, 0 );
    fdAdminCommand.right = new FormAttachment( 100, 0 );
    fdAdminCommand.top = new FormAttachment( wlAdminCommand, 0, SWT.CENTER );
    wAdminCommand.setLayoutData( fdAdminCommand );
    lastControl = wAdminCommand;

    // The base folder to run the command from
    //
    Label wlBaseFolder = new Label( wComposite, SWT.RIGHT );
    wlBaseFolder.setText( "Base folder (below import/ folder) " );
    props.setLook( wlBaseFolder );
    FormData fdlBaseFolder = new FormData();
    fdlBaseFolder.left = new FormAttachment( 0, 0 );
    fdlBaseFolder.right = new FormAttachment( middle, -margin );
    fdlBaseFolder.top = new FormAttachment( lastControl, 2 * margin );
    wlBaseFolder.setLayoutData( fdlBaseFolder );
    wBaseFolder = new TextVar(transMeta, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBaseFolder );
    wBaseFolder.addModifyListener( lsMod );
    FormData fdBaseFolder = new FormData();
    fdBaseFolder.left = new FormAttachment( middle, 0 );
    fdBaseFolder.right = new FormAttachment( 100, 0 );
    fdBaseFolder.top = new FormAttachment( wlBaseFolder, 0, SWT.CENTER );
    wBaseFolder.setLayoutData( fdBaseFolder );
    lastControl = wBaseFolder;

    Label wlStrategy = new Label( wComposite, SWT.RIGHT );
    wlStrategy.setText( "Node/Relationships Uniqueness strategy " );
    props.setLook( wlStrategy );
    FormData fdlStrategy = new FormData();
    fdlStrategy.left = new FormAttachment( 0, 0 );
    fdlStrategy.right = new FormAttachment( middle, -margin );
    fdlStrategy.top = new FormAttachment( lastControl, 2 * margin );
    wlStrategy.setLayoutData( fdlStrategy );
    wStrategy = new CCombo( wComposite, SWT.CHECK | SWT.BORDER );
    wStrategy.setItems( UniquenessStrategy.getNames() );
    props.setLook( wStrategy );
    FormData fdStrategy = new FormData();
    fdStrategy.left = new FormAttachment( middle, 0 );
    fdStrategy.right = new FormAttachment( 100, 0 );
    fdStrategy.top = new FormAttachment( wlStrategy, 0, SWT.CENTER );
    wStrategy.setLayoutData( fdStrategy );
    lastControl = wStrategy;
    
    // Some buttons
    wOK = new Button( wComposite, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( wComposite, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wCancel }, margin, lastControl );

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent( wComposite );

    wScrolledComposite.setExpandHorizontal( true );
    wScrolledComposite.setExpandVertical( true );
    wScrolledComposite.setMinWidth( bounds.width );
    wScrolledComposite.setMinHeight( bounds.height );

    // Add listeners
    //
    wCancel.addListener( SWT.Selection, e->cancel() );
    wOK.addListener( SWT.Selection, e->ok() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wConnection.addSelectionListener( lsDef );
    wGraphField.addSelectionListener( lsDef );
    wDatabaseFilename.addSelectionListener( lsDef );
    wAdminCommand.addSelectionListener( lsDef );
    wBaseFolder.addSelectionListener( lsDef );

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

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    input.setChanged( changed );

    shell.open();

    // Set the shell size, based upon previous time...
    setSize();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText( Const.NVL( input.getConnectionName(), "" ) );
    wGraphField.setText( Const.NVL( input.getGraphFieldName(), "" ) );
    wDatabaseFilename.setText( Const.NVL( input.getDatabaseFilename(), "" ) );
    wAdminCommand.setText( Const.NVL( input.getAdminCommand(), "" ) );
    wBaseFolder.setText( Const.NVL( input.getBaseFolder(), "" ) );
    if (input.getUniquenessStrategy()!=null) {
      int idx = Const.indexOfString( input.getUniquenessStrategy().name(), UniquenessStrategy.getNames() );
      wStrategy.select(idx);
    }

    // List of connections...
    //
    try {
      List<String> elementNames = NeoConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Neo4j connections", e );
    }
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(LoadMeta meta) {
    meta.setConnectionName( wConnection.getText() );
    meta.setGraphFieldName( wGraphField.getText() );
    meta.setAdminCommand( wAdminCommand.getText() );
    meta.setDatabaseFilename( wDatabaseFilename.getText() );
    meta.setBaseFolder( wBaseFolder.getText() );
    meta.setUniquenessStrategy( UniquenessStrategy.getStrategyFromName( wStrategy.getText() ) );
    System.out.println("Dialog Strategy="+meta.getUniquenessStrategy().name());
  }

  protected void newConnection() {
    NeoConnection connection = NeoConnectionUtils.newConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  protected void editConnection() {
    NeoConnectionUtils.editConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ), wConnection.getText() );
  }
}
