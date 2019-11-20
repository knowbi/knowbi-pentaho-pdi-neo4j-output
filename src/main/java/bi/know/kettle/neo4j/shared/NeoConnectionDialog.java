package bi.know.kettle.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.neo4j.kettle.shared.NeoConnection;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.CheckBoxVar;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog that allows you to edit the settings of a Neo4j connection
 *
 * @author Matt
 * @see NeoConnection
 */

public class NeoConnectionDialog {
  private static Class<?> PKG = NeoConnectionDialog.class; // for i18n purposes, needed by Translator2!!

  private NeoConnection neoConnection;

  private Shell parent;
  private Shell shell;

  // Connection properties
  //
  private Text wName;
  private Label wlServer;
  private TextVar wServer;
  private Label wlDatabaseName;
  private TextVar wDatabaseName;
  private Label wlBoltPort;
  private TextVar wBoltPort;
  private Label wlBrowserPort;
  private TextVar wBrowserPort;
  private Label wlPolicy;
  private TextVar wPolicy;
  private TextVar wUsername;
  private TextVar wPassword;
  private Label wlRouting;
  private CheckBoxVar wRouting;
  private Label wlEncryption;
  private Button wEncryption;

  Control lastControl;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  private Group gAdvanced;
  private TextVar wConnectionLivenessCheckTimeout;
  private TextVar wMaxConnectionLifetime;
  private TextVar wMaxConnectionPoolSize;
  private TextVar wConnectionAcquisitionTimeout;
  private TextVar wConnectionTimeout;
  private TextVar wMaxTransactionRetryTime;

  private TableView wUrls;
  private Button wOK;

  public NeoConnectionDialog( Shell parent, NeoConnection neoConnection ) {
    this.parent = parent;
    this.neoConnection = neoConnection;
    props = PropsUI.getInstance();
    ok = false;
  }

  public boolean open() {
    Display display = parent.getDisplay();
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

    // Buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, e -> ok() );

    Button wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "System.Button.Test" ) );
    wTest.addListener( SWT.Selection, e -> test() );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    Button[] buttons = new Button[] { wOK, wTest, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, lastControl );

    // Add the form widgets...
    //
    addFormWidgets();

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wUsername.addSelectionListener( selAdapter );
    wPassword.addSelectionListener( selAdapter );
    wServer.addSelectionListener( selAdapter );
    wDatabaseName.addSelectionListener( selAdapter );
    wBoltPort.addSelectionListener( selAdapter );
    wBrowserPort.addSelectionListener( selAdapter );
    wPolicy.addSelectionListener( selAdapter );
    wConnectionLivenessCheckTimeout.addSelectionListener( selAdapter );
    wMaxConnectionLifetime.addSelectionListener( selAdapter );
    wMaxConnectionPoolSize.addSelectionListener( selAdapter );
    wConnectionAcquisitionTimeout.addSelectionListener( selAdapter );
    wConnectionTimeout.addSelectionListener( selAdapter );
    wMaxTransactionRetryTime.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
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
    wlServer = new Label( shell, SWT.RIGHT );
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

    // The DatabaseName
    wlDatabaseName = new Label( shell, SWT.RIGHT );
    props.setLook( wlDatabaseName );
    wlDatabaseName.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.DatabaseName.Label" ) );
    FormData fdlDatabaseName = new FormData();
    fdlDatabaseName.top = new FormAttachment( lastControl, margin );
    fdlDatabaseName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDatabaseName.right = new FormAttachment( middle, -margin );
    wlDatabaseName.setLayoutData( fdlDatabaseName );
    wDatabaseName = new TextVar( neoConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatabaseName );
    FormData fdDatabaseName = new FormData();
    fdDatabaseName.top = new FormAttachment( wlDatabaseName, 0, SWT.CENTER );
    fdDatabaseName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdDatabaseName.right = new FormAttachment( 95, 0 );
    wDatabaseName.setLayoutData( fdDatabaseName );
    lastControl = wDatabaseName;

    //Bolt port?
    wlBoltPort = new Label( shell, SWT.RIGHT );
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
    wlBrowserPort = new Label( shell, SWT.RIGHT );
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
    wlRouting = new Label( shell, SWT.RIGHT );
    wlRouting.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.Routing.Label" ) );
    props.setLook( wlRouting );
    FormData fdlRouting = new FormData();
    fdlRouting.top = new FormAttachment( lastControl, margin );
    fdlRouting.left = new FormAttachment( 0, 0 );
    fdlRouting.right = new FormAttachment( middle, -margin );
    wlRouting.setLayoutData( fdlRouting );
    wRouting = new CheckBoxVar( neoConnection, shell, SWT.CHECK );
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
    wlEncryption = new Label( shell, SWT.RIGHT );
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

    gAdvanced = new Group(shell, SWT.SHADOW_ETCHED_IN);
    props.setLook( gAdvanced );
    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginTop = margin;
    advancedLayout.marginBottom = margin;
    gAdvanced.setLayout( advancedLayout );
    gAdvanced.setText("Advanced options");

    // ConnectionLivenessCheckTimeout
    Label wlConnectionLivenessCheckTimeout = new Label( gAdvanced, SWT.RIGHT );
    wlConnectionLivenessCheckTimeout.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.ConnectionLivenessCheckTimeout.Label" ) );
    props.setLook( wlConnectionLivenessCheckTimeout );
    FormData fdlConnectionLivenessCheckTimeout = new FormData();
    fdlConnectionLivenessCheckTimeout.top = new FormAttachment( 0, 0 );
    fdlConnectionLivenessCheckTimeout.left = new FormAttachment( 0, 0 );
    fdlConnectionLivenessCheckTimeout.right = new FormAttachment( middle, -margin );
    wlConnectionLivenessCheckTimeout.setLayoutData( fdlConnectionLivenessCheckTimeout );
    wConnectionLivenessCheckTimeout = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnectionLivenessCheckTimeout );
    FormData fdConnectionLivenessCheckTimeout = new FormData();
    fdConnectionLivenessCheckTimeout.top = new FormAttachment( wlConnectionLivenessCheckTimeout, 0, SWT.CENTER );
    fdConnectionLivenessCheckTimeout.left = new FormAttachment( middle, 0 );
    fdConnectionLivenessCheckTimeout.right = new FormAttachment( 95, 0 );
    wConnectionLivenessCheckTimeout.setLayoutData( fdConnectionLivenessCheckTimeout );
    Control lastGroupControl = wConnectionLivenessCheckTimeout;
    
    // MaxConnectionLifetime
    Label wlMaxConnectionLifetime = new Label( gAdvanced, SWT.RIGHT );
    wlMaxConnectionLifetime.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.MaxConnectionLifetime.Label" ) );
    props.setLook( wlMaxConnectionLifetime );
    FormData fdlMaxConnectionLifetime = new FormData();
    fdlMaxConnectionLifetime.top = new FormAttachment( lastGroupControl, margin );
    fdlMaxConnectionLifetime.left = new FormAttachment( 0, 0 );
    fdlMaxConnectionLifetime.right = new FormAttachment( middle, -margin );
    wlMaxConnectionLifetime.setLayoutData( fdlMaxConnectionLifetime );
    wMaxConnectionLifetime = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxConnectionLifetime );
    FormData fdMaxConnectionLifetime = new FormData();
    fdMaxConnectionLifetime.top = new FormAttachment( wlMaxConnectionLifetime, 0, SWT.CENTER );
    fdMaxConnectionLifetime.left = new FormAttachment( middle, 0 );
    fdMaxConnectionLifetime.right = new FormAttachment( 95, 0 );
    wMaxConnectionLifetime.setLayoutData( fdMaxConnectionLifetime );
    lastGroupControl = wMaxConnectionLifetime;

    // MaxConnectionPoolSize
    Label wlMaxConnectionPoolSize = new Label( gAdvanced, SWT.RIGHT );
    wlMaxConnectionPoolSize.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.MaxConnectionPoolSize.Label" ) );
    props.setLook( wlMaxConnectionPoolSize );
    FormData fdlMaxConnectionPoolSize = new FormData();
    fdlMaxConnectionPoolSize.top = new FormAttachment( lastGroupControl, margin );
    fdlMaxConnectionPoolSize.left = new FormAttachment( 0, 0 );
    fdlMaxConnectionPoolSize.right = new FormAttachment( middle, -margin );
    wlMaxConnectionPoolSize.setLayoutData( fdlMaxConnectionPoolSize );
    wMaxConnectionPoolSize = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxConnectionPoolSize );
    FormData fdMaxConnectionPoolSize = new FormData();
    fdMaxConnectionPoolSize.top = new FormAttachment( wlMaxConnectionPoolSize, 0, SWT.CENTER );
    fdMaxConnectionPoolSize.left = new FormAttachment( middle, 0 );
    fdMaxConnectionPoolSize.right = new FormAttachment( 95, 0 );
    wMaxConnectionPoolSize.setLayoutData( fdMaxConnectionPoolSize );
    lastGroupControl = wMaxConnectionPoolSize;

    // ConnectionAcquisitionTimeout
    Label wlConnectionAcquisitionTimeout = new Label( gAdvanced, SWT.RIGHT );
    wlConnectionAcquisitionTimeout.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.ConnectionAcquisitionTimeout.Label" ) );
    props.setLook( wlConnectionAcquisitionTimeout );
    FormData fdlConnectionAcquisitionTimeout = new FormData();
    fdlConnectionAcquisitionTimeout.top = new FormAttachment( lastGroupControl, margin );
    fdlConnectionAcquisitionTimeout.left = new FormAttachment( 0, 0 );
    fdlConnectionAcquisitionTimeout.right = new FormAttachment( middle, -margin );
    wlConnectionAcquisitionTimeout.setLayoutData( fdlConnectionAcquisitionTimeout );
    wConnectionAcquisitionTimeout = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnectionAcquisitionTimeout );
    FormData fdConnectionAcquisitionTimeout = new FormData();
    fdConnectionAcquisitionTimeout.top = new FormAttachment( wlConnectionAcquisitionTimeout, 0, SWT.CENTER );
    fdConnectionAcquisitionTimeout.left = new FormAttachment( middle, 0 );
    fdConnectionAcquisitionTimeout.right = new FormAttachment( 95, 0 );
    wConnectionAcquisitionTimeout.setLayoutData( fdConnectionAcquisitionTimeout );
    lastGroupControl = wConnectionAcquisitionTimeout;
    
    // ConnectionTimeout
    Label wlConnectionTimeout = new Label( gAdvanced, SWT.RIGHT );
    wlConnectionTimeout.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.ConnectionTimeout.Label" ) );
    props.setLook( wlConnectionTimeout );
    FormData fdlConnectionTimeout = new FormData();
    fdlConnectionTimeout.top = new FormAttachment( lastGroupControl, margin );
    fdlConnectionTimeout.left = new FormAttachment( 0, 0 );
    fdlConnectionTimeout.right = new FormAttachment( middle, -margin );
    wlConnectionTimeout.setLayoutData( fdlConnectionTimeout );
    wConnectionTimeout = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnectionTimeout );
    FormData fdConnectionTimeout = new FormData();
    fdConnectionTimeout.top = new FormAttachment( wlConnectionTimeout, 0, SWT.CENTER );
    fdConnectionTimeout.left = new FormAttachment( middle, 0 );
    fdConnectionTimeout.right = new FormAttachment( 95, 0 );
    wConnectionTimeout.setLayoutData( fdConnectionTimeout );
    lastGroupControl = wConnectionTimeout;
    
    // MaxTransactionRetryTime
    Label wlMaxTransactionRetryTime = new Label( gAdvanced, SWT.RIGHT );
    wlMaxTransactionRetryTime.setText( BaseMessages.getString( PKG, "NeoConnectionDialog.MaxTransactionRetryTime.Label" ) );
    props.setLook( wlMaxTransactionRetryTime );
    FormData fdlMaxTransactionRetryTime = new FormData();
    fdlMaxTransactionRetryTime.top = new FormAttachment( lastGroupControl, margin );
    fdlMaxTransactionRetryTime.left = new FormAttachment( 0, 0 );
    fdlMaxTransactionRetryTime.right = new FormAttachment( middle, -margin );
    wlMaxTransactionRetryTime.setLayoutData( fdlMaxTransactionRetryTime );
    wMaxTransactionRetryTime = new TextVar( neoConnection, gAdvanced, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxTransactionRetryTime );
    FormData fdMaxTransactionRetryTime = new FormData();
    fdMaxTransactionRetryTime.top = new FormAttachment( wlMaxTransactionRetryTime, 0, SWT.CENTER );
    fdMaxTransactionRetryTime.left = new FormAttachment( middle, 0 );
    fdMaxTransactionRetryTime.right = new FormAttachment( 95, 0 );
    wMaxTransactionRetryTime.setLayoutData( fdMaxTransactionRetryTime );
    lastGroupControl = wMaxTransactionRetryTime;

    // End of Advanced group
    //
    FormData fdgAdvanced = new FormData();
    fdgAdvanced.left = new FormAttachment( 0, 0 );
    fdgAdvanced.right = new FormAttachment( 100, 0 );
    fdgAdvanced.top = new FormAttachment( lastControl, margin*2 );
    gAdvanced.setLayoutData( fdgAdvanced );
    lastControl = gAdvanced;

    // The URLs group...
    //
    Group gUrls = new Group(shell, SWT.SHADOW_ETCHED_IN);
    props.setLook( gUrls );
    FormLayout urlsLayout = new FormLayout();
    urlsLayout.marginTop = margin;
    urlsLayout.marginBottom = margin;
    gUrls.setLayout( urlsLayout );
    gUrls.setText("Manual URLs");

    // URLs
    wUrls = new TableView( neoConnection, gUrls, SWT.NONE, new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "NeoConnectionDialog.URLColumn.Label" ), ColumnInfo.COLUMN_TYPE_TEXT )
      },
      neoConnection.getManualUrls().size(),
      null,
      props
    );
    wUrls.table.addListener( SWT.Selection, e->{enableFields();} );
    wUrls.table.addListener( SWT.MouseDown, e->{enableFields();} );
    wUrls.table.addListener( SWT.MouseUp, e->{enableFields();} );
    wUrls.table.addListener( SWT.FocusIn, e->{enableFields();} );
    wUrls.table.addListener( SWT.FocusOut, e->{enableFields();} );
    wUrls.addModifyListener( e->{enableFields();} );

    FormData fdUrls = new FormData();
    fdUrls.top = new FormAttachment( 0, 0 );
    fdUrls.left = new FormAttachment( 0, 0 );
    fdUrls.right = new FormAttachment( 100, 0 );
    fdUrls.bottom = new FormAttachment( 100, 0 );
    wUrls.setLayoutData( fdUrls );

    // End of Advanced group
    //
    FormData fdgUrls = new FormData();
    fdgUrls.left = new FormAttachment( 0, 0 );
    fdgUrls.right = new FormAttachment( 100, 0 );
    fdgUrls.top = new FormAttachment( lastControl, margin*2 );
    fdgUrls.bottom = new FormAttachment( wOK, -margin*2 );
    gUrls.setLayoutData( fdgUrls );
    lastControl = gUrls;
  }

  private void enableFields() {
    wlPolicy.setEnabled( wRouting.getSelection() );
    wPolicy.setEnabled( wRouting.getSelection() );

    boolean hasNoUrls = wUrls.nrNonEmpty()==0;
    for (Control control : new Control[] {
      wlServer, wServer, wlDatabaseName, wDatabaseName, wlBoltPort, wBoltPort, wlRouting, wRouting, wlPolicy, wPolicy, wlEncryption, wEncryption, gAdvanced}
      ) {
      control.setEnabled( hasNoUrls );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( neoConnection.getName(), "" ) );
    wServer.setText( Const.NVL( neoConnection.getServer(), "" ) );
    wDatabaseName.setText( Const.NVL( neoConnection.getDatabaseName(), "" ) );
    wBoltPort.setText( Const.NVL( neoConnection.getBoltPort(), "" ) );
    wBrowserPort.setText( Const.NVL( neoConnection.getBrowserPort(), "" ) );
    wRouting.setSelection( neoConnection.isRouting() );
    wRouting.setVariableName( Const.NVL(neoConnection.getRoutingVariable(), "") );
    wPolicy.setText( Const.NVL( neoConnection.getRoutingPolicy(), "" ) );
    wUsername.setText( Const.NVL( neoConnection.getUsername(), "" ) );
    wPassword.setText( Const.NVL( neoConnection.getPassword(), "" ) );
    wEncryption.setSelection( neoConnection.isUsingEncryption() );
    wConnectionLivenessCheckTimeout.setText( Const.NVL(neoConnection.getConnectionLivenessCheckTimeout(), "") );
    wMaxConnectionLifetime.setText( Const.NVL(neoConnection.getMaxConnectionLifetime(), "") );
    wMaxConnectionPoolSize.setText( Const.NVL(neoConnection.getMaxConnectionPoolSize(), "") );
    wConnectionAcquisitionTimeout.setText( Const.NVL(neoConnection.getConnectionAcquisitionTimeout(), "") );
    wConnectionTimeout.setText( Const.NVL(neoConnection.getConnectionTimeout(), "") );
    wMaxTransactionRetryTime.setText( Const.NVL(neoConnection.getMaxTransactionRetryTime(), "") );
    for (int i=0;i<neoConnection.getManualUrls().size();i++) {
      TableItem item = wUrls.table.getItem( i );
      item.setText(1, Const.NVL(neoConnection.getManualUrls().get( i ), ""));
    }
    wUrls.setRowNums();
    wUrls.optWidth( true );

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
    neo.setDatabaseName( wDatabaseName.getText() );
    neo.setBoltPort( wBoltPort.getText() );
    neo.setBrowserPort( wBrowserPort.getText() );
    neo.setRouting( wRouting.getSelection() );
    neo.setRoutingVariable( wRouting.getVariableName() );
    neo.setRoutingPolicy( wPolicy.getText() );
    neo.setUsername( wUsername.getText() );
    neo.setPassword( wPassword.getText() );
    neo.setUsingEncryption( wEncryption.getSelection() );

    neo.setConnectionLivenessCheckTimeout( wConnectionLivenessCheckTimeout.getText() );
    neo.setMaxConnectionLifetime( wMaxConnectionLifetime.getText() );
    neo.setMaxConnectionPoolSize( wMaxConnectionPoolSize.getText() );
    neo.setConnectionAcquisitionTimeout( wConnectionAcquisitionTimeout.getText() );
    neo.setConnectionTimeout( wConnectionTimeout.getText() );
    neo.setMaxTransactionRetryTime( wMaxTransactionRetryTime.getText() );

    neo.getManualUrls().clear();
    for (int i=0;i<wUrls.nrNonEmpty();i++){
      TableItem item = wUrls.getNonEmpty( i );
      neo.getManualUrls().add(item.getText( 1 ));
    }
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
