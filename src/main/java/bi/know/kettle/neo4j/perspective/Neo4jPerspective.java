package bi.know.kettle.neo4j.perspective;

import bi.know.kettle.neo4j.core.MetaStoreUtil;
import bi.know.kettle.neo4j.core.Neo4jDefaults;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.AuthenticationEvent;
import org.eclipse.swt.browser.AuthenticationListener;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.EngineMetaInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPerspectiveImageProvider;
import org.pentaho.di.ui.spoon.SpoonPerspectiveListener;
import org.pentaho.di.ui.spoon.SpoonPerspectiveManager;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulOverlay;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.components.XulTab;
import org.pentaho.ui.xul.components.XulTabpanel;
import org.pentaho.ui.xul.containers.XulTabbox;
import org.pentaho.ui.xul.containers.XulTabpanels;
import org.pentaho.ui.xul.containers.XulTabs;
import org.pentaho.ui.xul.containers.XulVbox;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.pentaho.ui.xul.swt.tags.SwtDeck;
import org.pentaho.ui.xul.swt.tags.SwtTab;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class Neo4jPerspective extends AbstractXulEventHandler implements SpoonPerspective, SpoonPerspectiveImageProvider, XulEventHandler {
  private static Class<?> PKG = Neo4jPerspective.class;
  private ResourceBundle resourceBundle = new XulSpoonResourceBundle( PKG );

  final String PERSPECTIVE_ID = "021-neo4j"; //$NON-NLS-1$
  final String PERSPECTIVE_NAME = "neo4jPerspective"; //$NON-NLS-1$

  private XulDomContainer container;
  private Neo4jController controller;
  private Neo4jMenuController gitSpoonMenuController;
  private XulVbox box;

  private LogChannelInterface logger = LogChannel.GENERAL;

  protected XulRunner runner;

  protected Document document;
  protected XulTabs tabs;
  protected XulTabpanels panels;
  protected XulTabbox tabbox;
  protected List<SpoonPerspectiveListener> listeners = new ArrayList<>();

  protected MetaStoreFactory<NeoConnection> connectionFactory;

  private static Neo4jPerspective instance;
  private org.eclipse.swt.widgets.List wConnections;

  public static Neo4jPerspective getInstance() throws XulException {
    if ( instance == null ) {
      instance = new Neo4jPerspective();
    }
    return instance;
  }

  private Neo4jPerspective() throws XulException {
    KettleXulLoader loader = new KettleXulLoader();
    loader.registerClassLoader( getClass().getClassLoader() );
    container = loader.loadXul( "neo4j_perspective.xul", resourceBundle );

    try {

      document = container.getDocumentRoot();
      tabs = (XulTabs) document.getElementById( "tabs" );
      panels = (XulTabpanels) document.getElementById( "tabpanels" );
      tabbox = (XulTabbox) tabs.getParent();

      container.addEventHandler( this );
      connectionFactory = new MetaStoreFactory<>( NeoConnection.class, Spoon.getInstance().getMetaStore(), Neo4jDefaults.NAMESPACE );

      addAdminTab();

      /*
       * To make compatible with webSpoon
       * Create a temporary parent for the UI and then call layout().
       * A different parent will be assigned to the UI in SpoonPerspectiveManager.PerspectiveManager.performInit().
       */
      SwtDeck deck = (SwtDeck) Spoon.getInstance().getXulDomContainer().getDocumentRoot().getElementById( "canvas-deck" );
      box = deck.createVBoxCard();
      getUI().setParent( (Composite) box.getManagedObject() );
      getUI().layout();

    } catch ( Exception e ) {
      logger.logError( "Error initializing perspective", e );
    }

  }

  private void addAdminTab() throws Exception {

    final XulTabAndPanel tabAndPanel = createTab();
    tabAndPanel.tab.setLabel( "Admin" );

    PropsUI props = PropsUI.getInstance();

    final Composite comp = (Composite) tabAndPanel.panel.getManagedObject();
    props.setLook( comp );
    comp.setLayout( new FillLayout() );

    ScrolledComposite scrolledComposite = new ScrolledComposite( comp, SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( scrolledComposite );
    scrolledComposite.setLayout( new FillLayout() );

    final Composite parentComposite = new Composite( scrolledComposite, SWT.NONE );
    props.setLook( parentComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginLeft = 10;
    formLayout.marginRight = 10;
    formLayout.marginTop = 10;
    formLayout.marginBottom = 10;
    formLayout.spacing = Const.MARGIN;
    parentComposite.setLayout( formLayout );

    // Add widgets...
    //
    Label wlConnections = new Label( parentComposite, SWT.LEFT | SWT.SINGLE );
    wlConnections.setText( "Neo4j Connections" );
    props.setLook( wlConnections );
    FormData fdlConnections = new FormData();
    fdlConnections.left = new FormAttachment( 0, 0 );
    fdlConnections.top = new FormAttachment( 0, 0 );
    wlConnections.setLayoutData( fdlConnections );

    wConnections = new org.eclipse.swt.widgets.List( parentComposite, SWT.BORDER | SWT.V_SCROLL );
    props.setLook( wConnections );
    updateConnectionsList();
    FormData fdConnections = new FormData();
    fdConnections.left = new FormAttachment( 0, 0 );
    fdConnections.top = new FormAttachment( wlConnections, Const.MARGIN );
    fdConnections.right = new FormAttachment( props.getMiddlePct(), 0 );
    fdConnections.bottom = new FormAttachment( 80, 0 );
    wConnections.setLayoutData( fdConnections );

    // Add a few buttons to create, edit, delete connections
    //
    Button wbOpen = new Button( parentComposite, SWT.PUSH );
    wbOpen.setText( "Open Connection" );
    props.setLook( wbOpen );
    Button wbNew = new Button( parentComposite, SWT.PUSH );
    wbNew.setText( "New Connection" );
    props.setLook( wbNew );
    Button wbEdit = new Button( parentComposite, SWT.PUSH );
    wbEdit.setText( "Edit Connection" );
    props.setLook( wbEdit );
    Button wbDelete = new Button( parentComposite, SWT.PUSH );
    wbDelete.setText( "Delete Connection" );
    props.setLook( wbDelete );

    FormData fdbOpen = new FormData();
    fdbOpen.left = new FormAttachment( props.getMiddlePct(), Const.MARGIN );
    fdbOpen.top = new FormAttachment( wlConnections, Const.MARGIN );
    fdbOpen.right = new FormAttachment( wbDelete, 0, SWT.RIGHT );
    wbOpen.setLayoutData( fdbOpen );
    wbOpen.addListener( SWT.Selection, this::openConnection );

    FormData fdbNew = new FormData();
    fdbNew.left = new FormAttachment( props.getMiddlePct(), Const.MARGIN );
    fdbNew.top = new FormAttachment( wbOpen, Const.MARGIN );
    fdbNew.right = new FormAttachment( wbDelete, 0, SWT.RIGHT );
    wbNew.setLayoutData( fdbNew );
    wbNew.addListener( SWT.Selection, this::newConnection );

    FormData fdbEdit = new FormData();
    fdbEdit.left = new FormAttachment( props.getMiddlePct(), Const.MARGIN );
    fdbEdit.top = new FormAttachment( wbNew, Const.MARGIN );
    fdbEdit.right = new FormAttachment( wbDelete, 0, SWT.RIGHT );
    wbEdit.setLayoutData( fdbEdit );
    wbEdit.addListener( SWT.Selection, this::editConnection );

    FormData fdbDelete = new FormData();
    fdbDelete.left = new FormAttachment( props.getMiddlePct(), Const.MARGIN );
    fdbDelete.top = new FormAttachment( wbEdit, Const.MARGIN );
    wbDelete.setLayoutData( fdbDelete );
    wbDelete.addListener( SWT.Selection, this::deleteConnection );

    parentComposite.layout( true );
    parentComposite.pack();

    // What's the size:
    Rectangle bounds = parentComposite.getBounds();

    scrolledComposite.setContent( parentComposite );
    scrolledComposite.setExpandHorizontal( true );
    scrolledComposite.setExpandVertical( true );
    scrolledComposite.setMinWidth( bounds.width );
    scrolledComposite.setMinHeight( bounds.height );

    comp.layout();
  }

  private void updateConnectionsList() throws MetaStoreException {
    wConnections.setItems( connectionFactory.getElementNames().toArray( new String[ 0 ] ) );
  }

  private void openConnection( Event event ) {
    String[] selection = wConnections.getSelection();
    if ( selection == null || selection.length < 1 ) {
      return;
    }
    String connectionName = selection[ 0 ];

    try {
      openConnection( connectionName );
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Unable to open Neo4j Browser to connection '" + connectionName + "'", e );
    }
  }

  private void newConnection( Event event ) {
    try {
      NeoConnection connection = NeoConnectionUtils.newConnection(
        Spoon.getInstance().getShell(),
        getVariableSpace(),
        NeoConnectionUtils.getConnectionFactory( Spoon.getInstance().getMetaStore() )
      );
      if ( connection != null ) {
        updateConnectionsList();
        wConnections.setSelection( new String[] { connection.getName() } );

      }
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Unable to create connection", e );
    }
  }

  private void editConnection( Event event ) {
    String[] selection = wConnections.getSelection();
    if ( selection == null || selection.length < 1 ) {
      return;
    }
    String connectionName = selection[ 0 ];

    try {
      NeoConnectionUtils.editConnection(
        Spoon.getInstance().getShell(),
        getVariableSpace(),
        connectionFactory,
        connectionName
      );
      updateConnectionsList();
      wConnections.setSelection( new String[] { connectionName } );
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Unable to edit connection", e );
    }

  }

  private void deleteConnection( Event event ) {
    String[] selection = wConnections.getSelection();
    if ( selection == null || selection.length < 1 ) {
      return;
    }
    String connectionName = selection[ 0 ];

    try {
      NeoConnectionUtils.deleteConnection(
        Spoon.getInstance().getShell(),
        connectionFactory,
        connectionName
      );
      updateConnectionsList();
    } catch ( Exception e ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Unable to delete connection", e );
    }
  }

  /**
   * Gets controller
   *
   * @return value of controller
   */
  public Neo4jController getController() {
    return this.controller;
  }

  @Override public String getPerspectiveIconPath() {
    return null;
  }

  @Override public String getId() {
    return PERSPECTIVE_ID;
  }

  @Override public Composite getUI() {
    return (Composite) container.getDocumentRoot().getRootElement().getFirstChild().getManagedObject();
  }

  @Override public String getDisplayName( Locale locale ) {
    return BaseMessages.getString( PKG, "Neo4j.Perspective.perspectiveName" );
  }

  @Override public InputStream getPerspectiveIcon() {
    return null;
  }

  @Override public void setActive( boolean active ) {
    for ( SpoonPerspectiveListener listener : listeners ) {
      if ( active ) {
        listener.onActivation();
      } else {
        listener.onDeactication();
      }
    }
  }

  public class XulTabAndPanel {
    public XulTab tab;
    public XulTabpanel panel;

    public XulTabAndPanel( XulTab tab, XulTabpanel panel ) {
      this.tab = tab;
      this.panel = panel;
    }
  }

  public XulTabAndPanel createTab() {

    try {
      XulTab tab = (XulTab) document.createElement( "tab" );
      XulTabpanel panel = (XulTabpanel) document.createElement( "tabpanel" );
      panel.setSpacing( 0 );
      panel.setPadding( 0 );

      tabs.addChild( tab );
      panels.addChild( panel );
      tabbox.setSelectedIndex( panels.getChildNodes().indexOf( panel ) );


      tab.addPropertyChangeListener( new PropertyChangeListener() {
        @Override public void propertyChange( PropertyChangeEvent evt ) {
          LogChannel.GENERAL.logBasic( "Property changed: " + evt.getPropertyName() + ", " + evt.toString() );
        }
      } );

      return new XulTabAndPanel( tab, panel );
    } catch ( XulException e ) {
      e.printStackTrace();
    }
    return null;
  }

  public void openConnection( String name ) throws Exception {

    IMetaStore metaStore = Spoon.getInstance().getMetaStore();
    MetaStoreFactory<NeoConnection> connectionFactory = new MetaStoreFactory<NeoConnection>( NeoConnection.class, metaStore, Neo4jDefaults.NAMESPACE );
    NeoConnection neoConnection = connectionFactory.loadElement( name );
    if ( neoConnection == null ) {
      new ErrorDialog( Spoon.getInstance().getShell(), "Error", "Error loading Neo4j connection '" + name + "'", new Exception() );
      return;
    }
    createTabForNeo4jConnection( neoConnection );


  }

  public void createTabForNeo4jConnection( final NeoConnection neoConnection ) throws Exception {
    // SpoonPerspectiveManager.getInstance().activatePerspective(getClass());

    final XulTabAndPanel tabAndPanel = createTab();
    PropsUI props = PropsUI.getInstance();

    final Composite comp = (Composite) tabAndPanel.panel.getManagedObject();
    props.setLook( comp );
    comp.setLayout( new FillLayout() );

    Browser browser = new Browser( comp, SWT.NONE );

    VariableSpace space = getVariableSpace();

    String server = space.environmentSubstitute( neoConnection.getServer() );
    String browserPortString = neoConnection.getBrowserPort();
    if ( StringUtils.isEmpty(browserPortString)) {
      browserPortString = "7474";
    }
    String browserPort = space.environmentSubstitute( browserPortString );

    final String browseUrl = "http://" + server + ":"+browserPort+"/browser/";
    LogChannel.GENERAL.logBasic( "Opening Neo4j Browser: "+browseUrl );
    browser.setUrl( browseUrl );

    setNameForTab( tabAndPanel.tab, neoConnection.getName() );
    setActive( true );

    comp.layout();

    // Spoon.getInstance().enableMenus();
  }

  private VariableSpace getVariableSpace() {
    VariableSpace space = (VariableSpace) Spoon.getInstance().getActiveMeta();
    if ( space == null ) {
      // Just use system variables
      //
      space = new Variables();
      space.initializeVariablesFrom( null );
    }
    return space;
  }

  public void setNameForTab( XulTab tab, String name ) {
    String tabName = name;
    List<String> usedNames = new ArrayList<String>();
    for ( XulComponent c : tabs.getChildNodes() ) {
      if ( c != tab ) {
        usedNames.add( ( (SwtTab) c ).getLabel() );
      }
    }
    if ( usedNames.contains( name ) ) {
      int num = 2;
      while ( true ) {
        tabName = name + " (" + num + ")";
        if ( usedNames.contains( tabName ) == false ) {
          break;
        }
        num++;
      }
    }

    tab.setLabel( tabName );
  }

  @Override public List<XulOverlay> getOverlays() {
    return null;
  }

  @Override public List<XulEventHandler> getEventHandlers() {
    return null;
  }

  @Override public void addPerspectiveListener( SpoonPerspectiveListener spoonPerspectiveListener ) {

  }

  @Override public EngineMetaInterface getActiveMeta() {
    return null;
  }

  @Override public String getName() {
    return "neo4jPerspective";
  }

  public boolean onTabClose( final int pos ) throws XulException {
    // Never close the admin tab
    //
    if ( pos == 0 ) {
      return false;
    }
    return true;
  }

}
