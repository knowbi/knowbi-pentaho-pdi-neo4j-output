package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog that allows you to edit the settings of a Neo4j connection
 *
 * @author Matt
 * @see NeoConnection
 */

public class NeoConnectionDialog extends Dialog {
  private static Class<?> PKG = NeoConnectionDialog.class; // for i18n purposes, needed by Translator2!!

  private NeoConnection neoConnection;

  private Shell shell;

  // Connection properties
  //
  private Text wName;
  private TextVar wServer;
  private TextVar wBoltPort;
  private TextVar wBrowserPort;
  private TextVar wPolicy;
  private TextVar wUsername;
  private TextVar wPassword;
  private Button wRouting, wEncryption;

  Control lastControl;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;
  private Label wlPolicy;

  public NeoConnectionDialog( Shell par, NeoConnection neoConnection ) {
    super( par, SWT.NONE );
    this.neoConnection = neoConnection;
    props = PropsUI.getInstance();
    ok = false;
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSlave() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    addFormWidgets();

    // Buttons
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    Button wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "System.Button.Test" ) );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wTest, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, lastControl );

    // Add listeners
    wOK.addListener( SWT.Selection, e -> ok() );
    wTest.addListener( SWT.Selection, e -> test() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wUsername.addSelectionListener( selAdapter );
    wPassword.addSelectionListener( selAdapter );
    wServer.addSelectionListener( selAdapter );
    wBoltPort.addSelectionListener( selAdapter );
    wBrowserPort.addSelectionListener( selAdapter );
    wPolicy.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  private void addFormWidgets() {

    // The name
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    lastControl = wName;

    // The server
    Label wlServer = new Label( shell, SWT.RIGHT );
    props.setLook( wlServer );
    wlServer.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Server.Label" ) );
    FormData fdlServer = new FormData();
    fdlServer.top = new FormAttachment( lastControl, margin );
    fdlServer.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlServer.right = new FormAttachment( middle, -margin );
    wlServer.setLayoutData( fdlServer );
    wServer = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wServer );
    FormData fdServer = new FormData();
    fdServer.top = new FormAttachment( wlServer, 0, SWT.CENTER );
    fdServer.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdServer.right = new FormAttachment( 95, 0 );
    wServer.setLayoutData( fdServer );
    lastControl = wServer;

    //Bolt port?
    Label wlBoltPort = new Label( shell, SWT.RIGHT );
    props.setLook( wlBoltPort );
    wlBoltPort.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.BoltPort.Label" ) );
    FormData fdlBoltPort = new FormData();
    fdlBoltPort.top = new FormAttachment( lastControl, margin );
    fdlBoltPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlBoltPort.right = new FormAttachment( middle, -margin );
    wlBoltPort.setLayoutData( fdlBoltPort );
    wBoltPort = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBoltPort );
    FormData fdBoltPort = new FormData();
    fdBoltPort.top = new FormAttachment( wlBoltPort, 0, SWT.CENTER );
    fdBoltPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdBoltPort.right = new FormAttachment( 95, 0 );
    wBoltPort.setLayoutData( fdBoltPort );
    lastControl = wBoltPort;

    //Browser port?
    Label wlBrowserPort = new Label( shell, SWT.RIGHT );
    props.setLook( wlBrowserPort );
    wlBrowserPort.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.BrowserPort.Label" ) );
    FormData fdlBrowserPort = new FormData();
    fdlBrowserPort.top = new FormAttachment( lastControl, margin );
    fdlBrowserPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlBrowserPort.right = new FormAttachment( middle, -margin );
    wlBrowserPort.setLayoutData( fdlBrowserPort );
    wBrowserPort = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBrowserPort );
    FormData fdBrowserPort = new FormData();
    fdBrowserPort.top = new FormAttachment( wlBrowserPort, 0, SWT.CENTER );
    fdBrowserPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdBrowserPort.right = new FormAttachment( 95, 0 );
    wBrowserPort.setLayoutData( fdBrowserPort );
    lastControl = wBrowserPort;

    // Https
    Label wlRouting = new Label( shell, SWT.RIGHT );
    wlRouting.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Routing.Label" ) );
    props.setLook( wlRouting );
    FormData fdlRouting = new FormData();
    fdlRouting.top = new FormAttachment( lastControl, margin );
    fdlRouting.left = new FormAttachment( 0, 0 );
    fdlRouting.right = new FormAttachment( middle, -margin );
    wlRouting.setLayoutData( fdlRouting );
    wRouting = new Button( shell, SWT.CHECK );
    props.setLook( wRouting );
    FormData fdRouting = new FormData();
    fdRouting.top = new FormAttachment( wlRouting, 0, SWT.CENTER );
    fdRouting.left = new FormAttachment( middle, 0 );
    fdRouting.right = new FormAttachment( 95, 0 );
    wRouting.setLayoutData( fdRouting );
    lastControl = wRouting;

    // Policy
    wlPolicy = new Label( shell, SWT.RIGHT );
    wlPolicy.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Policy.Label" ) );
    props.setLook( wlPolicy );
    FormData fdlPolicy = new FormData();
    fdlPolicy.top = new FormAttachment( lastControl, margin );
    fdlPolicy.left = new FormAttachment( 0, 0 );
    fdlPolicy.right = new FormAttachment( middle, -margin );
    wlPolicy.setLayoutData( fdlPolicy );
    wPolicy = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPolicy );
    FormData fdPolicy = new FormData();
    fdPolicy.top = new FormAttachment( wlPolicy, 0, SWT.CENTER );
    fdPolicy.left = new FormAttachment( middle, 0 );
    fdPolicy.right = new FormAttachment( 95, 0 );
    wPolicy.setLayoutData( fdPolicy );
    lastControl = wPolicy;

    wRouting.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        enableFields();
      }
    } );


    // Username
    Label wlUsername = new Label( shell, SWT.RIGHT );
    wlUsername.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.UserName.Label" ) );
    props.setLook( wlUsername );
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment( lastControl, margin );
    fdlUsername.left = new FormAttachment( 0, 0 );
    fdlUsername.right = new FormAttachment( middle, -margin );
    wlUsername.setLayoutData( fdlUsername );
    wUsername = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUsername );
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment( wlUsername, 0, SWT.CENTER );
    fdUsername.left = new FormAttachment( middle, 0 );
    fdUsername.right = new FormAttachment( 95, 0 );
    wUsername.setLayoutData( fdUsername );
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label( shell, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment( wUsername, margin );
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );
    wPassword = new PasswordTextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment( wlPassword, 0, SWT.CENTER );
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.right = new FormAttachment( 95, 0 );
    wPassword.setLayoutData( fdPassword );
    lastControl = wPassword;

    // Encryption?
    Label wlEncryption = new Label( shell, SWT.RIGHT );
    wlEncryption.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Encryption.Label" ) );
    props.setLook( wlEncryption );
    FormData fdlEncryption = new FormData();
    fdlEncryption.top = new FormAttachment( lastControl, margin );
    fdlEncryption.left = new FormAttachment( 0, 0 );
    fdlEncryption.right = new FormAttachment( middle, -margin );
    wlEncryption.setLayoutData( fdlEncryption );
    wEncryption = new Button( shell, SWT.CHECK );
    props.setLook( wEncryption );
    FormData fdEncryption = new FormData();
    fdEncryption.top = new FormAttachment( wlEncryption, 0, SWT.CENTER );
    fdEncryption.left = new FormAttachment( middle, 0 );
    fdEncryption.right = new FormAttachment( 95, 0 );
    wEncryption.setLayoutData( fdEncryption );
    lastControl = wEncryption;
  }

  private void enableFields() {
    wlPolicy.setEnabled( wRouting.getSelection() );
    wPolicy.setEnabled( wRouting.getSelection() );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( neoConnection.getName(), "" ) );
    wServer.setText( Const.NVL( neoConnection.getServer(), "" ) );
    wBoltPort.setText( Const.NVL( neoConnection.getBoltPort(), "" ) );
    wRouting.setSelection( neoConnection.isRouting() );
    wPolicy.setText( Const.NVL( neoConnection.getRoutingPolicy(), "" ) );
    wUsername.setText( Const.NVL( neoConnection.getUsername(), "" ) );
    wPassword.setText( Const.NVL( neoConnection.getPassword(), "" ) );
    wEncryption.setSelection( neoConnection.isUsingEncryption() );

    enableFields();
    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {
    if ( StringUtils.isEmpty( wName.getText() ) ) {
      MessageBox box = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      box.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.NoNameDialog.Title" ) );
      box.setMessage( BaseMessages.getString( PKG, "NeoConnectionDialog.NoNameDialog.Message" ) );
      box.open();
      return;
    }
    getInfo( neoConnection );
    ok = true;
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( NeoConnection neo ) {
    neo.setName( wName.getText() );
    neo.setServer( wServer.getText() );
    neo.setBoltPort( wBoltPort.getText() );
    neo.setRouting( wRouting.getSelection() );
    neo.setRoutingPolicy( wPolicy.getText() );
    neo.setUsername( wUsername.getText() );
    neo.setPassword( wPassword.getText() );
    neo.setUsingEncryption( wEncryption.getSelection() );
  }

  public void test() {
    NeoConnection neo = new NeoConnection( neoConnection ); // parent as variable space
    try {
      getInfo( neo );
      neo.test();
      MessageBox box = new MessageBox( shell, SWT.OK );
      box.setText( "OK" );
      String message = "Connection successful!" + Const.CR;
      message += Const.CR;
      message += "URL : " + neo.getUrl();
      box.setMessage( message );
      box.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error connecting to Neo with URL : " + neo.getUrl(), e );
    }
  }
}
