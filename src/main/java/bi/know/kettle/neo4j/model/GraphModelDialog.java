package bi.know.kettle.neo4j.model;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterListDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.Date;

public class GraphModelDialog extends Dialog {

  private static Class<?> PKG = GraphModelDialog.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabs;

  private Shell shell;
  private PropsUI props;

  private boolean ok;
  private int margin;
  private int middle;

  // Model fields
  private Text wModelDescription;
  private Text wModelName;

  // Node fields
  private List wNodesList;
  private Text wNodeName;
  private Text wNodeDescription;
  private TableView wNodeLabels;
  private boolean monitorLabels = true;
  private TableView wNodeProperties;
  private boolean monitorNodeProperties = true;

  // Relationship fields
  private List wRelationshipsList;
  private Text wRelName;
  private Text wRelDescription;
  private Text wRelLabel;
  private CCombo wRelSource;
  private CCombo wRelTarget;
  private TableView wRelProperties;
  private boolean monitorRelProperties = true;

  private GraphModel originalGraphModel;
  private GraphModel graphModel;
  private GraphNode activeNode;
  private GraphRelationship activeRelationship;
  private Button wImportNode;

  private RowMetaInterface inputRowMeta;

  public GraphModelDialog( Shell parent, GraphModel graphModel ) {
    this( parent, graphModel, null );
  }

  public GraphModelDialog( Shell parent, GraphModel graphModel, RowMetaInterface inputRowMeta ) {
    super( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.CLOSE );
    this.inputRowMeta = inputRowMeta;

    props = PropsUI.getInstance();
    ok = false;

    // We will only replace at OK
    //
    this.originalGraphModel = graphModel;

    // We will change graphModel
    //
    this.graphModel = graphModel.clone();

  }

  /**
   * @return true when OK is hit, false when CANCEL
   */
  public boolean open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.DIALOG_TRIM );
    shell.setImage( GUIResource.getInstance().getImageSpoon() );
    props.setLook( shell );

    margin = Const.MARGIN + 2;
    middle = Const.MIDDLE_PCT;

    FormLayout formLayout = new FormLayout();

    shell.setLayout( formLayout );
    shell.setText( "Graph Model Editor" );

    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, event -> ok() );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, event -> cancel() );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

    // Add a tab folder
    //
    wTabs = new CTabFolder( shell, SWT.BORDER );
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment( 0, 0 );
    fdTabs.right = new FormAttachment( 100, 0 );
    fdTabs.top = new FormAttachment( 0, 0 );
    fdTabs.bottom = new FormAttachment( wOK, -margin * 2 );
    wTabs.setLayoutData( fdTabs );

    addModelTab();
    addNodesTab();
    addRelationshipsTab();
    addGraphTab();

    // Select the model tab
    //
    wTabs.setSelection( 0 );

    // Set the shell size, based upon previous time...
    BaseStepDialog.setSize( shell );

    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return ok;
  }

  private void ok() {

    ok = true;
    originalGraphModel.replace( graphModel );
    dispose();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  private void getData() {
    // Model tab
    wModelName.setText( Const.NVL( graphModel.getName(), "" ) );
    wModelName.addListener( SWT.Modify, event -> graphModel.setName( wModelName.getText() ) );
    wModelDescription.setText( Const.NVL( graphModel.getDescription(), "" ) );
    wModelDescription.addListener( SWT.Modify, event -> graphModel.setDescription( wModelDescription.getText() ) );

    refreshNodesList();
    if ( graphModel.getNodes().size() > 0 ) {
      String activeName = graphModel.getNodeNames()[ 0 ];
      activeNode = graphModel.findNode( activeName );
      wNodesList.setSelection( new String[] { activeName } );
      refreshNodeFields();
    }
    refreshRelationshipsList();
    if ( graphModel.getRelationships().size() > 0 ) {
      String activeRelationshipName = graphModel.getRelationshipNames()[ 0 ];
      activeRelationship = graphModel.findRelationship( activeRelationshipName );
      wRelationshipsList.setSelection( new String[] { activeRelationshipName } );
      refreshRelationshipsFields();
    }
  }

  private void setActiveNode( String nodeName ) {
    activeNode = graphModel.findNode( nodeName );
  }

  private void setActiveRelationship( String relationshipName ) {
    activeRelationship = graphModel.findRelationship( relationshipName );
  }


  private void refreshNodeFields() {
    if ( activeNode == null ) {
      return;
    }

    wNodeName.setText( Const.NVL( activeNode.getName(), "" ) );
    wNodeDescription.setText( Const.NVL( activeNode.getDescription(), "" ) );

    // Refresh the Labels
    //
    monitorLabels = false;
    System.out.println( "Adding " + activeNode.getLabels().size() + " labels to the labels view" );
    wNodeLabels.clearAll();
    for ( int i = 0; i < activeNode.getLabels().size(); i++ ) {
      TableItem item = new TableItem( wNodeLabels.table, SWT.NONE );
      item.setText( 1, activeNode.getLabels().get( i ) );
    }
    wNodeLabels.removeEmptyRows( 1 );
    wNodeLabels.setRowNums();
    wNodeLabels.optWidth( true );
    monitorLabels = true;

    // Refresh the Properties
    //
    monitorNodeProperties = false;
    System.out.println( "Adding " + activeNode.getProperties().size() + " properties to the node view" );
    wNodeProperties.clearAll();
    for ( int i = 0; i < activeNode.getProperties().size(); i++ ) {
      GraphProperty property = activeNode.getProperties().get( i );
      TableItem item = new TableItem( wNodeProperties.table, SWT.NONE );
      item.setText( 1, Const.NVL( property.getName(), "" ) );
      item.setText( 2, GraphPropertyType.getCode( property.getType() ) );
      item.setText( 3, Const.NVL( property.getDescription(), "" ) );
      item.setText( 4, property.isPrimary() ? "Y" : "N" );
    }
    wNodeProperties.removeEmptyRows( 1 );
    wNodeProperties.setRowNums();
    wNodeProperties.optWidth( true );

    wNodeName.setFocus();
    monitorNodeProperties = true;

  }

  private void refreshRelationshipsFields() {
    if ( activeRelationship == null ) {
      return;
    }

    wRelName.setText( Const.NVL( activeRelationship.getName(), "" ) );
    wRelDescription.setText( Const.NVL( activeRelationship.getDescription(), "" ) );
    wRelLabel.setText( Const.NVL( activeRelationship.getLabel(), "" ) );
    wRelSource.setText( Const.NVL( activeRelationship.getNodeSource(), "" ) );
    wRelTarget.setText( Const.NVL( activeRelationship.getNodeTarget(), "" ) );

    // Refresh the Properties
    //
    monitorRelProperties = false;
    System.out.println( "Adding " + activeRelationship.getProperties().size() + " properties to the relationship view" );
    wRelProperties.clearAll();
    for ( int i = 0; i < activeRelationship.getProperties().size(); i++ ) {
      GraphProperty property = activeRelationship.getProperties().get( i );
      TableItem item = new TableItem( wRelProperties.table, SWT.NONE );
      item.setText( 1, Const.NVL( property.getName(), "" ) );
      item.setText( 2, GraphPropertyType.getCode( property.getType() ) );
      item.setText( 3, Const.NVL( property.getDescription(), "" ) );
      item.setText( 4, property.isPrimary() ? "Y" : "N" );
    }
    wRelProperties.removeEmptyRows( 1 );
    wRelProperties.setRowNums();
    wRelProperties.optWidth( true );

    wRelName.setFocus();

    monitorRelProperties = true;
  }

  private void refreshNodesList() {
    String[] nodeNames = graphModel.getNodeNames();
    wNodesList.setItems( nodeNames );
    if ( activeNode != null ) {
      wNodesList.setSelection( new String[] { activeNode.getName(), } );
    }
    wRelSource.setItems( nodeNames );
    wRelTarget.setItems( nodeNames );
  }

  private void refreshRelationshipsList() {
    // Relationships tab
    //
    wRelationshipsList.setItems( graphModel.getRelationshipNames() );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void addModelTab() {

    CTabItem wModelTab = new CTabItem( wTabs, SWT.NONE );
    wModelTab.setText( "Model" );

    ScrolledComposite wModelSComp = new ScrolledComposite( wTabs, SWT.V_SCROLL | SWT.H_SCROLL );
    wModelSComp.setLayout( new FillLayout() );

    Composite wModelComp = new Composite( wModelSComp, SWT.NONE );
    props.setLook( wModelComp );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wModelComp.setLayout( formLayout );

    // Model properties
    //  - Name
    //  - Description
    //
    Label wlName = new Label( wModelComp, SWT.RIGHT );
    wlName.setText( "Model name" );
    props.setLook( wlName );
    FormData fdlModelName = new FormData();
    fdlModelName.left = new FormAttachment( 0, 0 );
    fdlModelName.right = new FormAttachment( middle, 0 );
    fdlModelName.top = new FormAttachment( 0, 0 );
    wlName.setLayoutData( fdlModelName );
    wModelName = new Text( wModelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModelName );
    FormData fdModelName = new FormData();
    fdModelName.left = new FormAttachment( middle, margin );
    fdModelName.right = new FormAttachment( 100, 0 );
    fdModelName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    wModelName.setLayoutData( fdModelName );
    Control lastControl = wModelName;

    Label wlModelDescription = new Label( wModelComp, SWT.RIGHT );
    wlModelDescription.setText( "Model description" );
    props.setLook( wlModelDescription );
    FormData fdlModelDescription = new FormData();
    fdlModelDescription.left = new FormAttachment( 0, 0 );
    fdlModelDescription.right = new FormAttachment( middle, 0 );
    fdlModelDescription.top = new FormAttachment( lastControl, margin );
    wlModelDescription.setLayoutData( fdlModelDescription );
    wModelDescription = new Text( wModelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModelDescription );
    FormData fdModelDescription = new FormData();
    fdModelDescription.left = new FormAttachment( middle, margin );
    fdModelDescription.right = new FormAttachment( 100, 0 );
    fdModelDescription.top = new FormAttachment( lastControl, 0, SWT.CENTER );
    wModelDescription.setLayoutData( fdModelDescription );
    lastControl = wModelDescription;

    Button wImportGraph = new Button( wModelComp, SWT.PUSH );
    wImportGraph.setText( "Import graph from JSON" );
    props.setLook( wImportGraph );
    FormData fdImportGraph = new FormData();
    fdImportGraph.left = new FormAttachment( middle, 0 );
    fdImportGraph.top = new FormAttachment( lastControl, 50 );
    wImportGraph.setLayoutData( fdImportGraph );
    wImportGraph.addListener( SWT.Selection, ( e ) -> importGraphFromFile() );
    lastControl = wImportGraph;

    Button wExportGraph = new Button( wModelComp, SWT.PUSH );
    wExportGraph.setText( "Export graph to JSON" );
    props.setLook( wExportGraph );
    FormData fdExportGraph = new FormData();
    fdExportGraph.left = new FormAttachment( middle, 0 );
    fdExportGraph.top = new FormAttachment( lastControl, margin );
    wExportGraph.setLayoutData( fdExportGraph );
    wExportGraph.addListener( SWT.Selection, ( e ) -> exportGraphToFile() );

    FormData fdModelComp = new FormData();
    fdModelComp.left = new FormAttachment( 0, 0 );
    fdModelComp.top = new FormAttachment( 0, 0 );
    fdModelComp.right = new FormAttachment( 100, 0 );
    fdModelComp.bottom = new FormAttachment( 100, 0 );
    wModelComp.setLayoutData( fdModelComp );

    wModelComp.pack();

    Rectangle bounds = wModelComp.getBounds();

    wModelSComp.setContent( wModelComp );
    wModelSComp.setExpandHorizontal( true );
    wModelSComp.setExpandVertical( true );
    wModelSComp.setMinWidth( bounds.width );
    wModelSComp.setMinHeight( bounds.height );

    wModelTab.setControl( wModelSComp );
  }


  private void addNodesTab() {

    CTabItem wNodesTab = new CTabItem( wTabs, SWT.NONE );
    wNodesTab.setText( "Nodes" );

    ScrolledComposite wNodesSComp = new ScrolledComposite( wTabs, SWT.V_SCROLL | SWT.H_SCROLL );
    wNodesSComp.setLayout( new FillLayout() );

    Composite wNodesComp = new Composite( wNodesSComp, SWT.NONE );
    props.setLook( wNodesComp );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    wNodesComp.setLayout( formLayout );

    // Nodes properties
    //  - Nodes List
    //  - Node name
    //  - Node description
    //  - Node labels
    //  - Node properties list
    //

    // buttons for New/Edit/Delete Node
    //
    Button wNewNode = new Button( wNodesComp, SWT.PUSH );
    wNewNode.setText( "New node" );
    wNewNode.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newNode();
      }
    } );

    Button wDeleteNode = new Button( wNodesComp, SWT.PUSH );
    wDeleteNode.setText( "Delete node" );
    wDeleteNode.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        deleteNode();
      }
    } );

    Button wCopyNode = new Button( wNodesComp, SWT.PUSH );
    wCopyNode.setText( "Copy node" );
    wCopyNode.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        copyNode();
      }
    } );

    wImportNode = new Button( wNodesComp, SWT.PUSH );
    wImportNode.setText( "Import properties" );
    wImportNode.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        importNodeProperties();
      }
    } );
    if ( inputRowMeta == null ) {
      wImportNode.setEnabled( false );
    }

    BaseStepDialog.positionBottomButtons( wNodesComp, new Button[] { wNewNode, wDeleteNode, wCopyNode, wImportNode }, margin, null );


    Label wlNodesList = new Label( wNodesComp, SWT.LEFT );
    wlNodesList.setText( "Nodes list" );
    props.setLook( wlNodesList );
    FormData fdlNodesList = new FormData();
    fdlNodesList.left = new FormAttachment( 0, 0 );
    fdlNodesList.right = new FormAttachment( middle, 0 );
    fdlNodesList.top = new FormAttachment( 0, 0 );
    wlNodesList.setLayoutData( fdlNodesList );
    wNodesList = new List( wNodesComp, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER );
    props.setLook( wNodesList );
    FormData fdNodesList = new FormData();
    fdNodesList.left = new FormAttachment( 0, 0 );
    fdNodesList.right = new FormAttachment( middle, 0 );
    fdNodesList.top = new FormAttachment( wlNodesList, margin );
    fdNodesList.bottom = new FormAttachment( wNewNode, -margin * 2 );
    wNodesList.setLayoutData( fdNodesList );
    wNodesList.addListener( SWT.Selection, event -> {
      setActiveNode( wNodesList.getSelection()[ 0 ] );
      refreshNodeFields();
    } );

    Label wlNodeName = new Label( wNodesComp, SWT.RIGHT );
    wlNodeName.setText( "Name" );
    props.setLook( wlNodeName );
    FormData fdlNodeName = new FormData();
    fdlNodeName.left = new FormAttachment( middle, margin );
    fdlNodeName.top = new FormAttachment( wlNodesList, margin * 2 );
    wlNodeName.setLayoutData( fdlNodeName );
    wNodeName = new Text( wNodesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNodeName.addListener( SWT.Modify, event -> {
      if ( activeNode != null ) {
        activeNode.setName( wNodeName.getText() );
        if ( wNodesList.getSelectionIndex() >= 0 ) {
          wNodesList.setItem( wNodesList.getSelectionIndex(), wNodeName.getText() );
        }
      }
    } );
    props.setLook( wNodeName );
    FormData fdNodeName = new FormData();
    fdNodeName.left = new FormAttachment( wlNodeName, margin * 2 );
    fdNodeName.right = new FormAttachment( 100, 0 );
    fdNodeName.top = new FormAttachment( wlNodeName, 0, SWT.CENTER );
    wNodeName.setLayoutData( fdNodeName );

    Label wlNodeDescription = new Label( wNodesComp, SWT.RIGHT );
    wlNodeDescription.setText( "Description" );
    props.setLook( wlNodeDescription );
    FormData fdlNodeDescription = new FormData();
    fdlNodeDescription.left = new FormAttachment( middle, margin );
    fdlNodeDescription.top = new FormAttachment( wNodeName, margin );
    wlNodeDescription.setLayoutData( fdlNodeDescription );
    wNodeDescription = new Text( wNodesComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNodeDescription.addListener( SWT.Modify, event -> {
      if ( activeNode != null ) {
        activeNode.setDescription( wNodeDescription.getText() );
      }
    } );
    props.setLook( wNodeDescription );
    FormData fdNodeDescription = new FormData();
    fdNodeDescription.left = new FormAttachment( wlNodeDescription, margin * 2 );
    fdNodeDescription.right = new FormAttachment( 100, 0 );
    fdNodeDescription.top = new FormAttachment( wlNodeDescription, 0, SWT.CENTER );
    wNodeDescription.setLayoutData( fdNodeDescription );

    // Labels
    //
    ColumnInfo[] labelColumns = new ColumnInfo[] {
      new ColumnInfo( "Labels", ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };
    ModifyListener labelModifyListener = modifyEvent -> getNodeLabelsFromView();
    wNodeLabels =
      new TableView( new Variables(), wNodesComp, SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER, labelColumns, 1, labelModifyListener, props );
    props.setLook( wNodeLabels );
    FormData fdNodeLabels = new FormData();
    fdNodeLabels.left = new FormAttachment( middle, margin );
    fdNodeLabels.right = new FormAttachment( 100, 0 );
    fdNodeLabels.top = new FormAttachment( wNodeDescription, margin );
    fdNodeLabels.bottom = new FormAttachment( wNodeDescription, 150 + margin );
    wNodeLabels.setLayoutData( fdNodeLabels );

    // Properties
    //
    Label wlNodeProperties = new Label( wNodesComp, SWT.LEFT );
    wlNodeProperties.setText( "Properties:" );
    props.setLook( wlNodeProperties );
    FormData fdlNodeProperties = new FormData();
    fdlNodeProperties.left = new FormAttachment( middle, margin );
    fdlNodeProperties.top = new FormAttachment( wNodeLabels, margin );
    wlNodeProperties.setLayoutData( fdlNodeProperties );

    ColumnInfo[] propertyColumns = new ColumnInfo[] {
      new ColumnInfo( "Property key", ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( "Property Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false ),
      new ColumnInfo( "Description", ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( "Primary?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" }, false ),
    };
    ModifyListener propertyModifyListener = modifyEvent -> getNodePropertiesFromView();
    wNodeProperties =
      new TableView( new Variables(), wNodesComp, SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER, propertyColumns, 1, propertyModifyListener, props );
    wNodeProperties.table.addListener( SWT.FocusOut, event -> getNodePropertiesFromView() );
    props.setLook( wNodeProperties );
    FormData fdNodeProperties = new FormData();
    fdNodeProperties.left = new FormAttachment( middle, margin );
    fdNodeProperties.right = new FormAttachment( 100, 0 );
    fdNodeProperties.top = new FormAttachment( wlNodeProperties, margin );
    fdNodeProperties.bottom = new FormAttachment( wNewNode, -2 * margin );
    wNodeProperties.setLayoutData( fdNodeProperties );

    FormData fdNodesComp = new FormData();
    fdNodesComp.left = new FormAttachment( 0, 0 );
    fdNodesComp.top = new FormAttachment( 0, 0 );
    fdNodesComp.right = new FormAttachment( 100, 0 );
    fdNodesComp.bottom = new FormAttachment( 100, 0 );
    wNodesComp.setLayoutData( fdNodesComp );

    wNodesComp.pack();

    Rectangle bounds = wNodesComp.getBounds();

    wNodesSComp.setContent( wNodesComp );
    wNodesSComp.setExpandHorizontal( true );
    wNodesSComp.setExpandVertical( true );
    wNodesSComp.setMinWidth( bounds.width );
    wNodesSComp.setMinHeight( bounds.height );

    wNodesTab.setControl( wNodesSComp );
  }

  /**
   * Someone changed something in the labels, update the active node
   */
  private void getNodeLabelsFromView() {

    if ( activeNode != null ) {
      if ( monitorLabels ) {
        System.out.println( "Labels changed! " + new Date().getTime() + " found " + wNodeLabels.nrNonEmpty() + " labels" );

        java.util.List<String> labels = new ArrayList<>();
        for ( int i = 0; i < wNodeLabels.nrNonEmpty(); i++ ) {
          labels.add( wNodeLabels.getNonEmpty( i ).getText( 1 ) );
        }

        TableEditor editor = wNodeLabels.getEditor();
        if ( editor != null && editor.getEditor() != null && ( editor.getEditor() instanceof Text ) ) {
          Text text = (Text) editor.getEditor();
          if ( !text.isDisposed() ) {
            labels.add( text.getText() );
            System.out.println( "editor content : " + text.getText() );
          }
        }

        activeNode.setLabels( labels );
        System.out.println( "Set " + activeNode.getLabels().size() + " labels on active node" );
      }
    }
  }

  /**
   * Someone changed something in the properties, update the active node
   */
  private void getNodePropertiesFromView() {

    if ( activeNode != null ) {
      if ( monitorNodeProperties ) {
        System.out.println( "Labels changed! " + new Date().getTime() + " found " + wNodeProperties.nrNonEmpty() + " properties" );

        java.util.List<GraphProperty> properties = new ArrayList<>();
        for ( int i = 0; i < wNodeProperties.nrNonEmpty(); i++ ) {
          TableItem item = wNodeProperties.getNonEmpty( i );
          String propertyKey = item.getText( 1 );
          GraphPropertyType propertyType = GraphPropertyType.parseCode( item.getText( 2 ) );
          String propertyDescription = item.getText( 3 );
          boolean propertyPrimary = "Y".equalsIgnoreCase( item.getText( 4 ) );
          properties.add( new GraphProperty( propertyKey, propertyDescription, propertyType, propertyPrimary ) );
        }

        activeNode.setProperties( properties );
        System.out.println( "Set " + activeNode.getProperties().size() + " properties on active node" );
      }
    }
  }

  private void importNodeProperties() {
    if ( activeNode == null || inputRowMeta == null ) {
      return;
    }
    String[] fieldNames = inputRowMeta.getFieldNames();

    EnterListDialog dialog = new EnterListDialog( shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.CLOSE, fieldNames );
    String[] fields = dialog.open();
    if ( fields != null ) {

      for ( String field : fields ) {
        // add this field as a property...
        //
        ValueMetaInterface valueMeta = inputRowMeta.searchValueMeta( field );
        GraphPropertyType propertyType;
        switch ( valueMeta.getType() ) {
          case ValueMetaInterface.TYPE_INTEGER:
            propertyType = GraphPropertyType.Integer;
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            propertyType = GraphPropertyType.Float;
            break;
          case ValueMetaInterface.TYPE_DATE:
            propertyType = GraphPropertyType.LocalDateTime;
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            propertyType = GraphPropertyType.Boolean;
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            propertyType = GraphPropertyType.LocalDateTime;
            break;
          case ValueMetaInterface.TYPE_BINARY:
            propertyType = GraphPropertyType.ByteArray;
            break;
          default:
            propertyType = GraphPropertyType.String;
            break;
        }

        activeNode.getProperties().add( new GraphProperty( field, "", propertyType, false ) );
      }
      refreshNodeFields();
    }
  }

  private void copyNode() {
    if ( activeNode == null ) {
      return;
    }
    GraphNode graphNode = activeNode.clone();
    graphNode.setName( activeNode.getName() + " (copy)" );
    graphModel.getNodes().add( graphNode );
    activeNode = graphNode;
    refreshNodesList();
    wNodesList.setSelection( new String[] { graphNode.getName(), } );
    refreshNodeFields();
  }

  private void deleteNode() {
    if ( activeNode == null ) {
      return;
    }
    int selectionIndex = wNodesList.getSelectionIndex();
    graphModel.getNodes().remove( activeNode );
    refreshNodesList();
    if ( selectionIndex >= 0 ) {
      if ( selectionIndex >= wNodesList.getItemCount() ) {
        // Select last item if last was deleted.
        //
        selectionIndex = wNodesList.getItemCount() - 1;
      }
      wNodesList.setSelection( selectionIndex );
      setActiveNode( wNodesList.getSelection()[ 0 ] );
      refreshNodeFields();
    }
  }

  private void newNode() {
    GraphNode node = new GraphNode();
    node.setName( "Node " + ( graphModel.getNodes().size() + 1 ) );
    graphModel.getNodes().add( node );
    activeNode = node;
    refreshNodesList();
    refreshNodeFields();
  }

  private void addRelationshipsTab() {

    CTabItem wRelTab = new CTabItem( wTabs, SWT.NONE );
    wRelTab.setText( "Relationships" );

    ScrolledComposite wRelSComp = new ScrolledComposite( wTabs, SWT.V_SCROLL | SWT.H_SCROLL );
    wRelSComp.setLayout( new FillLayout() );

    Composite wRelComp = new Composite( wRelSComp, SWT.NONE );
    props.setLook( wRelComp );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wRelComp.setLayout( formLayout );

    // Relationships properties
    //  - Relationships List
    //  - Relationship name
    //  - Relationship description
    //  - Relationship labels
    //  - Relationship properties list
    //

    // buttons for New/Edit/Delete Relationship
    //
    Button wNewRelationship = new Button( wRelComp, SWT.PUSH );
    wNewRelationship.setText( "New relationship" );
    wNewRelationship.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newRelationship();
      }
    } );

    Button wDeleteRelationship = new Button( wRelComp, SWT.PUSH );
    wDeleteRelationship.setText( "Delete relationship" );
    wDeleteRelationship.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        deleteRelationship();
      }
    } );

    Button wCopyRelationship = new Button( wRelComp, SWT.PUSH );
    wCopyRelationship.setText( "Copy relationship" );
    wCopyRelationship.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        copyRelationship();
      }
    } );
    BaseStepDialog.positionBottomButtons( wRelComp, new Button[] { wNewRelationship, wDeleteRelationship, wCopyRelationship }, margin, null );

    Label wlRelationshipsList = new Label( wRelComp, SWT.LEFT );
    wlRelationshipsList.setText( "Relationships list" );
    props.setLook( wlRelationshipsList );
    FormData fdlRelationshipsList = new FormData();
    fdlRelationshipsList.left = new FormAttachment( 0, 0 );
    fdlRelationshipsList.right = new FormAttachment( middle, 0 );
    fdlRelationshipsList.top = new FormAttachment( 0, margin );
    wlRelationshipsList.setLayoutData( fdlRelationshipsList );
    wRelationshipsList = new List( wRelComp, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER );
    props.setLook( wRelationshipsList );
    FormData fdRelationshipsList = new FormData();
    fdRelationshipsList.left = new FormAttachment( 0, 0 );
    fdRelationshipsList.right = new FormAttachment( middle, 0 );
    fdRelationshipsList.top = new FormAttachment( wlRelationshipsList, margin );
    fdRelationshipsList.bottom = new FormAttachment( wNewRelationship, -margin * 2 );
    wRelationshipsList.setLayoutData( fdRelationshipsList );
    wRelationshipsList.addListener( SWT.Selection, event -> {
      setActiveRelationship( wRelationshipsList.getSelection()[ 0 ] );
      refreshRelationshipsFields();
    } );

    Label wlRelName = new Label( wRelComp, SWT.LEFT );
    wlRelName.setText( "Name" );
    props.setLook( wlRelName );
    FormData fdlRelName = new FormData();
    fdlRelName.left = new FormAttachment( middle, margin );
    fdlRelName.top = new FormAttachment( wlRelationshipsList, 2 * margin );
    wlRelName.setLayoutData( fdlRelName );
    wRelName = new Text( wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelName );
    FormData fdRelName = new FormData();
    fdRelName.left = new FormAttachment( wlRelName, margin * 2 );
    fdRelName.right = new FormAttachment( 100, 0 );
    fdRelName.top = new FormAttachment( wlRelName, 0, SWT.CENTER );
    wRelName.setLayoutData( fdRelName );
    wRelName.addListener( SWT.Modify, event -> {
      if ( activeRelationship != null ) {
        activeRelationship.setName( wRelName.getText() );
        if ( wRelationshipsList.getSelectionIndex() >= 0 ) {
          wRelationshipsList.setItem( wRelationshipsList.getSelectionIndex(), wRelName.getText() );
        }
      }
    } );

    Label wlRelDescription = new Label( wRelComp, SWT.LEFT );
    wlRelDescription.setText( "Description" );
    props.setLook( wlRelDescription );
    FormData fdlRelDescription = new FormData();
    fdlRelDescription.left = new FormAttachment( middle, margin );
    fdlRelDescription.top = new FormAttachment( wRelName, margin );
    wlRelDescription.setLayoutData( fdlRelDescription );
    wRelDescription = new Text( wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelDescription );
    FormData fdRelDescription = new FormData();
    fdRelDescription.left = new FormAttachment( wlRelDescription, margin * 2 );
    fdRelDescription.right = new FormAttachment( 100, 0 );
    fdRelDescription.top = new FormAttachment( wlRelDescription, 0, SWT.CENTER );
    wRelDescription.setLayoutData( fdRelDescription );
    wRelDescription.addListener( SWT.Modify, event -> {
      if ( activeRelationship != null ) {
        activeRelationship.setDescription( wRelDescription.getText() );
      }
    } );

    Label wlRelLabel = new Label( wRelComp, SWT.LEFT );
    wlRelLabel.setText( "Label" );
    props.setLook( wlRelLabel );
    FormData fdlRelLabel = new FormData();
    fdlRelLabel.left = new FormAttachment( middle, margin );
    fdlRelLabel.top = new FormAttachment( wRelDescription, margin );
    wlRelLabel.setLayoutData( fdlRelLabel );
    wRelLabel = new Text( wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelLabel );
    FormData fdRelLabel = new FormData();
    fdRelLabel.left = new FormAttachment( wlRelLabel, margin * 2 );
    fdRelLabel.right = new FormAttachment( 100, 0 );
    fdRelLabel.top = new FormAttachment( wlRelLabel, 0, SWT.CENTER );
    wRelLabel.setLayoutData( fdRelLabel );
    wRelLabel.addListener( SWT.Modify, event -> {
      if ( activeRelationship != null ) {
        activeRelationship.setLabel( wRelLabel.getText() );
      }
    } );

    Label wlRelSource = new Label( wRelComp, SWT.LEFT );
    wlRelSource.setText( "Source" );
    props.setLook( wlRelSource );
    FormData fdlRelSource = new FormData();
    fdlRelSource.left = new FormAttachment( middle, margin );
    fdlRelSource.top = new FormAttachment( wRelLabel, margin );
    wlRelSource.setLayoutData( fdlRelSource );
    wRelSource = new CCombo( wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelSource );
    FormData fdRelSource = new FormData();
    fdRelSource.left = new FormAttachment( wlRelSource, margin * 2 );
    fdRelSource.right = new FormAttachment( 100, 0 );
    fdRelSource.top = new FormAttachment( wlRelSource, 0, SWT.CENTER );
    wRelSource.setLayoutData( fdRelSource );
    wRelSource.addListener( SWT.Modify, event -> {
      if ( activeRelationship != null ) {
        activeRelationship.setNodeSource( wRelSource.getText() );
      }
    } );

    Label wlRelTarget = new Label( wRelComp, SWT.LEFT );
    wlRelTarget.setText( "Target" );
    props.setLook( wlRelTarget );
    FormData fdlRelTarget = new FormData();
    fdlRelTarget.left = new FormAttachment( middle, margin );
    fdlRelTarget.top = new FormAttachment( wRelSource, margin );
    wlRelTarget.setLayoutData( fdlRelTarget );
    wRelTarget = new CCombo( wRelComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelTarget );
    FormData fdRelTarget = new FormData();
    fdRelTarget.left = new FormAttachment( wlRelTarget, margin * 2 );
    fdRelTarget.right = new FormAttachment( 100, 0 );
    fdRelTarget.top = new FormAttachment( wlRelTarget, 0, SWT.CENTER );
    wRelTarget.setLayoutData( fdRelTarget );
    wRelTarget.addListener( SWT.Modify, event -> {
      if ( activeRelationship != null ) {
        activeRelationship.setNodeTarget( wRelTarget.getText() );
      }
    } );


    // Properties
    //
    Label wlRelProperties = new Label( wRelComp, SWT.LEFT );
    wlRelProperties.setText( "Properties:" );
    props.setLook( wlRelProperties );
    FormData fdlRelProperties = new FormData();
    fdlRelProperties.left = new FormAttachment( middle, margin );
    fdlRelProperties.top = new FormAttachment( wRelTarget, margin );
    wlRelProperties.setLayoutData( fdlRelProperties );

    ColumnInfo[] propertyColumns = new ColumnInfo[] {
      new ColumnInfo( "Property key", ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( "Property Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false ),
      new ColumnInfo( "Description", ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( "Primary?", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" }, false ),
    };
    ModifyListener propertyModifyListener = modifyEvent -> getRelationshipPropertiesFromView();
    wRelProperties =
      new TableView( new Variables(), wRelComp, SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER, propertyColumns, 1, propertyModifyListener, props );
    wRelProperties.table.addListener( SWT.FocusOut, event -> getRelationshipPropertiesFromView() );
    props.setLook( wRelProperties );
    FormData fdRelProperties = new FormData();
    fdRelProperties.left = new FormAttachment( middle, margin );
    fdRelProperties.right = new FormAttachment( 100, 0 );
    fdRelProperties.top = new FormAttachment( wlRelProperties, margin );
    fdRelProperties.bottom = new FormAttachment( wNewRelationship, -2 * margin );
    wRelProperties.setLayoutData( fdRelProperties );

    FormData fdRelComp = new FormData();
    fdRelComp.left = new FormAttachment( 0, 0 );
    fdRelComp.top = new FormAttachment( 0, 0 );
    fdRelComp.right = new FormAttachment( 100, 0 );
    fdRelComp.bottom = new FormAttachment( 100, 0 );
    wRelComp.setLayoutData( fdRelComp );

    wRelComp.pack();

    Rectangle bounds = wRelComp.getBounds();

    wRelSComp.setContent( wRelComp );
    wRelSComp.setExpandHorizontal( true );
    wRelSComp.setExpandVertical( true );
    wRelSComp.setMinWidth( bounds.width );
    wRelSComp.setMinHeight( bounds.height );

    wRelTab.setControl( wRelSComp );
  }

  /**
   * Someone changed something in the relationship properties, update the active node
   */
  private void getRelationshipPropertiesFromView() {

    if ( activeRelationship != null ) {
      if ( monitorRelProperties ) {
        System.out.println( "Relationship properties changed! " + new Date().getTime() + " found " + wRelProperties.nrNonEmpty() + " properties" );

        java.util.List<GraphProperty> properties = new ArrayList<>();
        for ( int i = 0; i < wRelProperties.nrNonEmpty(); i++ ) {
          TableItem item = wRelProperties.getNonEmpty( i );
          String propertyKey = item.getText( 1 );
          GraphPropertyType propertyType = GraphPropertyType.parseCode( item.getText( 2 ) );
          String propertyDescription = item.getText( 3 );
          boolean propertyPrimary = "Y".equalsIgnoreCase( item.getText( 4 ) );
          properties.add( new GraphProperty( propertyKey, propertyDescription, propertyType, propertyPrimary ) );
        }

        activeRelationship.setProperties( properties );
        System.out.println( "Set " + activeRelationship.getProperties().size() + " properties on active relationship" );
      }
    }
  }

  private void copyRelationship() {
    if ( activeRelationship == null ) {
      return;
    }
    GraphRelationship graphRelationship = activeRelationship.clone();
    graphRelationship.setName( activeRelationship.getName() + " (copy)" );
    graphModel.getRelationships().add( graphRelationship );
    activeRelationship = graphRelationship;
    refreshRelationshipsList();
    wRelationshipsList.setSelection( new String[] { graphRelationship.getName(), } );
    refreshRelationshipsFields();
  }

  private void deleteRelationship() {
    if ( activeRelationship == null ) {
      return;
    }
    int selectionIndex = wRelationshipsList.getSelectionIndex();
    graphModel.getRelationships().remove( activeRelationship );
    refreshRelationshipsList();
    if ( selectionIndex >= 0 ) {
      if ( selectionIndex >= wRelationshipsList.getItemCount() ) {
        // Select last item if last was deleted.
        //
        selectionIndex = wRelationshipsList.getItemCount() - 1;
      }
      wRelationshipsList.setSelection( selectionIndex );
      setActiveRelationship( wRelationshipsList.getSelection()[ 0 ] );
      refreshRelationshipsFields();
    }
  }

  private void newRelationship() {
    GraphRelationship relationship = new GraphRelationship();
    relationship.setName( "Relationship " + ( graphModel.getRelationships().size() + 1 ) );
    graphModel.getRelationships().add( relationship );
    activeRelationship = relationship;
    refreshRelationshipsList();
    refreshRelationshipsFields();
  }

  private void addGraphTab() {

    CTabItem wGraphTab = new CTabItem( wTabs, SWT.NONE );
    wGraphTab.setText( "Graph" );

    ScrolledComposite wGraphSComp = new ScrolledComposite( wTabs, SWT.V_SCROLL | SWT.H_SCROLL );
    wGraphSComp.setLayout( new FillLayout() );

    Composite wGraphComp = new Composite( wGraphSComp, SWT.NONE );
    props.setLook( wGraphComp );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 3;
    formLayout.marginHeight = 3;
    wGraphComp.setLayout( formLayout );

    // Model properties
    //  - Name
    //  - Description
    //
    Label wlTodo = new Label( wGraphComp, SWT.RIGHT );
    wlTodo.setText( "TODO" );
    props.setLook( wlTodo );
    FormData fdlTodo = new FormData();
    fdlTodo.left = new FormAttachment( 0, 0 );
    fdlTodo.right = new FormAttachment( middle, 0 );
    fdlTodo.top = new FormAttachment( 0, 0 );
    wlTodo.setLayoutData( fdlTodo );


    FormData fdGraphComp = new FormData();
    fdGraphComp.left = new FormAttachment( 0, 0 );
    fdGraphComp.top = new FormAttachment( 0, 0 );
    fdGraphComp.right = new FormAttachment( 100, 0 );
    fdGraphComp.bottom = new FormAttachment( 100, 0 );
    wGraphComp.setLayoutData( fdGraphComp );

    wGraphComp.pack();

    Rectangle bounds = wGraphComp.getBounds();

    wGraphSComp.setContent( wGraphComp );
    wGraphSComp.setExpandHorizontal( true );
    wGraphSComp.setExpandVertical( true );
    wGraphSComp.setMinWidth( bounds.width );
    wGraphSComp.setMinHeight( bounds.height );

    wGraphTab.setControl( wGraphSComp );
  }


  public RowMetaInterface getInputRowMeta() {
    return inputRowMeta;
  }

  public void setInputRowMeta( RowMetaInterface inputRowMeta ) {
    this.inputRowMeta = inputRowMeta;
    if ( wImportNode != null && !wImportNode.isDisposed() ) {
      wImportNode.setEnabled( inputRowMeta != null );
    }
  }

  private void importGraphFromFile() {
    try {
      EnterTextDialog dialog = new EnterTextDialog( shell, "Model JSON", "This is the JSON of the graph model", graphModel.getJSONString(), true );
      String jsonModelString = dialog.open();
      if (jsonModelString==null) {
        return;
      }

      GraphModel importedGraphMode = new GraphModel(jsonModelString);

      // The graph model is loaded, replace the one in memory
      //
      graphModel = importedGraphMode;

      // Refresh the dialog.
      //
      getData();

    } catch ( Exception e ) {
      new ErrorDialog( shell, "ERROR", "Error importing JSON", e );
    }
  }

  private void exportGraphToFile() {
    try {
      String prettyJsonString = getModelJson();

      EnterTextDialog dialog = new EnterTextDialog( shell, "Model JSON", "This is the JSON of the graph model", prettyJsonString, true );
      dialog.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "ERROR", "Error serializing to JSON", e );
    }
  }

  private String getModelJson() throws KettleException {
    return graphModel.getJSONString();
  }



}
