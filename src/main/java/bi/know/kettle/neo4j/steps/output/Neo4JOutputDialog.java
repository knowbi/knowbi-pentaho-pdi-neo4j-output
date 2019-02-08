package bi.know.kettle.neo4j.steps.output;


import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.WordUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.Collections;
import java.util.List;


public class Neo4JOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = Neo4JOutputDialog.class; // for i18n purposes, needed by Translator2!!

  private Neo4JOutputMeta input;

  private Label wlConnection;
  private CCombo wConnection;
  private Label wlBatchSize;
  private TextVar wBatchSize;
  private Label wlCreateIndexes;
  private Button wCreateIndexes;
  private Label wlUseCreate;
  private Button wUseCreate;
  private Label wlOnlyCreateRelationships;
  private Button wOnlyCreateRelationships;
  private Button wReturnGraph;
  private Label wlReturnGraphField;
  private TextVar wReturnGraphField;

  private Combo wRel;
  private TextVar wRelValue;
  private TableView wFromPropsGrid;
  private TableView wFromLabelGrid;
  private TableView wToPropsGrid;
  private TableView wToLabelGrid;
  private TableView wRelPropsGrid;
  private String[] fieldNames;
  private Button wEditConnection;
  private Button wNewConnection;

  private Button wReadOnlyFromNode;
  private Button wReadOnlyToNode;

  public Neo4JOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (Neo4JOutputMeta) in;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }


  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Fields
    try {
      RowMetaInterface prevFields = transMeta.getPrevStepFields( stepname );
      fieldNames = prevFields.getFieldNames();
    } catch ( KettleStepException kse ) {
      logError( BaseMessages.getString( PKG, "TripleOutput.Log.ErrorGettingFieldNames" ) );
      fieldNames = new String[] {};
    }

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.Shell.Title" ) ); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.StepName.Label" ) ); //$NON-NLS-1$
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, margin );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;


    wlConnection = new Label( shell, SWT.RIGHT );
    wlConnection.setText( "Neo4j Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    wEditConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    wNewConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

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

    wConnection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;

    wlBatchSize = new Label( shell, SWT.RIGHT );
    wlBatchSize.setText( "Batch size (rows)" );
    props.setLook( wlBatchSize );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    fdlBatchSize.top = new FormAttachment( lastControl, 2 * margin );
    wlBatchSize.setLayoutData( fdlBatchSize );
    wBatchSize = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( middle, 0 );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 0, SWT.CENTER );
    wBatchSize.setLayoutData( fdBatchSize );
    lastControl = wBatchSize;

    wlCreateIndexes = new Label( shell, SWT.RIGHT );
    wlCreateIndexes.setText( "Create indexes? " );
    props.setLook( wlCreateIndexes );
    FormData fdlCreateIndexes = new FormData();
    fdlCreateIndexes.left = new FormAttachment( 0, 0 );
    fdlCreateIndexes.right = new FormAttachment( middle, -margin );
    fdlCreateIndexes.top = new FormAttachment( lastControl, 2 * margin );
    wlCreateIndexes.setLayoutData( fdlCreateIndexes );
    wCreateIndexes = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wCreateIndexes );
    FormData fdCreateIndexes = new FormData();
    fdCreateIndexes.left = new FormAttachment( middle, 0 );
    fdCreateIndexes.right = new FormAttachment( 100, 0 );
    fdCreateIndexes.top = new FormAttachment( wlCreateIndexes, 0, SWT.CENTER );
    wCreateIndexes.setLayoutData( fdCreateIndexes );
    lastControl = wCreateIndexes;

    wlUseCreate = new Label( shell, SWT.RIGHT );
    wlUseCreate.setText( "Use CREATE instead of MERGE? " );
    props.setLook( wlUseCreate );
    FormData fdlUseCreate = new FormData();
    fdlUseCreate.left = new FormAttachment( 0, 0 );
    fdlUseCreate.right = new FormAttachment( middle, -margin );
    fdlUseCreate.top = new FormAttachment( lastControl, 2 * margin );
    wlUseCreate.setLayoutData( fdlUseCreate );
    wUseCreate = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wUseCreate );
    FormData fdUseCreate = new FormData();
    fdUseCreate.left = new FormAttachment( middle, 0 );
    fdUseCreate.right = new FormAttachment( 100, 0 );
    fdUseCreate.top = new FormAttachment( wlUseCreate, 0, SWT.CENTER );
    wUseCreate.setLayoutData( fdUseCreate );
    lastControl = wUseCreate;

    wlOnlyCreateRelationships = new Label( shell, SWT.RIGHT );
    wlOnlyCreateRelationships.setText( "Only create relationships? " );
    props.setLook( wlOnlyCreateRelationships );
    FormData fdlOnlyCreateRelationships = new FormData();
    fdlOnlyCreateRelationships.left = new FormAttachment( 0, 0 );
    fdlOnlyCreateRelationships.right = new FormAttachment( middle, -margin );
    fdlOnlyCreateRelationships.top = new FormAttachment( lastControl, 2 * margin );
    wlOnlyCreateRelationships.setLayoutData( fdlOnlyCreateRelationships );
    wOnlyCreateRelationships = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wOnlyCreateRelationships );
    FormData fdOnlyCreateRelationships = new FormData();
    fdOnlyCreateRelationships.left = new FormAttachment( middle, 0 );
    fdOnlyCreateRelationships.right = new FormAttachment( 100, 0 );
    fdOnlyCreateRelationships.top = new FormAttachment( wlOnlyCreateRelationships, 0, SWT.CENTER );
    wOnlyCreateRelationships.setLayoutData( fdOnlyCreateRelationships );
    lastControl = wOnlyCreateRelationships;

    Label wlReturnGraph = new Label( shell, SWT.RIGHT );
    wlReturnGraph.setText( "Return graph data?" );
    String returnGraphTooltipText = "The update data to be updated in the form of Graph a value in the output of this step";
    wlReturnGraph.setToolTipText( returnGraphTooltipText );
    props.setLook( wlReturnGraph );
    FormData fdlReturnGraph = new FormData();
    fdlReturnGraph.left = new FormAttachment( 0, 0 );
    fdlReturnGraph.right = new FormAttachment( middle, -margin );
    fdlReturnGraph.top = new FormAttachment( lastControl, 2 * margin );
    wlReturnGraph.setLayoutData( fdlReturnGraph );
    wReturnGraph = new Button( shell, SWT.CHECK | SWT.BORDER );
    wReturnGraph.setToolTipText( returnGraphTooltipText );
    props.setLook( wReturnGraph );
    FormData fdReturnGraph = new FormData();
    fdReturnGraph.left = new FormAttachment( middle, 0 );
    fdReturnGraph.right = new FormAttachment( 100, 0 );
    fdReturnGraph.top = new FormAttachment( wlReturnGraph, 0, SWT.CENTER );
    wReturnGraph.setLayoutData( fdReturnGraph );
    wReturnGraph.addListener(SWT.Selection, e-> enableFields());
    lastControl = wReturnGraph;

    wlReturnGraphField = new Label( shell, SWT.RIGHT );
    wlReturnGraphField.setText( "Graph output field name" );
    props.setLook( wlReturnGraphField );
    FormData fdlReturnGraphField = new FormData();
    fdlReturnGraphField.left = new FormAttachment( 0, 0 );
    fdlReturnGraphField.right = new FormAttachment( middle, -margin );
    fdlReturnGraphField.top = new FormAttachment( lastControl, 2 * margin );
    wlReturnGraphField.setLayoutData( fdlReturnGraphField );
    wReturnGraphField = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReturnGraphField );
    wReturnGraphField.addModifyListener( lsMod );
    FormData fdReturnGraphField = new FormData();
    fdReturnGraphField.left = new FormAttachment( middle, 0 );
    fdReturnGraphField.right = new FormAttachment( 100, 0 );
    fdReturnGraphField.top = new FormAttachment( wlReturnGraphField, 0, SWT.CENTER );
    wReturnGraphField.setLayoutData( fdReturnGraphField );
    lastControl = wReturnGraphField;

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

    // Add listeners
    //
    lsCancel = e -> cancel();
    lsOK = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( lastControl, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin );
    wTabFolder.setLayoutData( fdTabFolder );


    /*
     * STRING_FROM
     */
    CTabItem wFromTab = new CTabItem( wTabFolder, SWT.NONE );
    wFromTab.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromTab" ) ); //$NON-NLS-1$

    FormLayout fromLayout = new FormLayout();
    fromLayout.marginWidth = 3;
    fromLayout.marginHeight = 3;

    Composite wFromComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFromComp );
    wFromComp.setLayout( fromLayout );


    // Read only "from" node?
    //
    Label wlReadOnlyFromNode = new Label( wFromComp, SWT.RIGHT );
    wlReadOnlyFromNode.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.ReadOnlyFromNode" ) );
    props.setLook( wlReadOnlyFromNode );
    FormData fdlReadOnlyFromNodes = new FormData();
    fdlReadOnlyFromNodes.left = new FormAttachment( 0, 0 );
    fdlReadOnlyFromNodes.right = new FormAttachment( middle, 0 );
    fdlReadOnlyFromNodes.top = new FormAttachment( 0, margin * 3 );
    wlReadOnlyFromNode.setLayoutData( fdlReadOnlyFromNodes );
    wReadOnlyFromNode = new Button(wFromComp, SWT.CHECK);
    props.setLook( wReadOnlyFromNode );
    FormData fdReadOnlyFromNode = new FormData();
    fdReadOnlyFromNode.left = new FormAttachment( middle, margin );
    fdReadOnlyFromNode.right = new FormAttachment( 100, 0 );
    fdReadOnlyFromNode.top = new FormAttachment( 0, margin * 3 );
    wReadOnlyFromNode.setLayoutData( fdReadOnlyFromNode );
    Control lastFromControl = wReadOnlyFromNode;

    // Labels
    Label wlFromLabel = new Label( wFromComp, SWT.RIGHT );
    wlFromLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.FromLabel" ) );
    props.setLook( wlFromLabel );
    FormData fdlFromLabels = new FormData();
    fdlFromLabels.left = new FormAttachment( 0, 0 );
    fdlFromLabels.right = new FormAttachment( middle, 0 );
    fdlFromLabels.top = new FormAttachment( lastFromControl, margin );
    wlFromLabel.setLayoutData( fdlFromLabels );
    final int fromLabelRows = ( input.getFromNodeLabels() != null ? input.getFromNodeLabels().length : 10 );
    ColumnInfo[] fromLabelInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromLabelsTable.FromFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromLabelsTable.FromValues" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };
    fromLabelInf[1].setUsingVariables( true );
    wFromLabelGrid = new TableView( Variables.getADefaultVariableSpace(), wFromComp, SWT.BORDER
      | SWT.FULL_SELECTION | SWT.MULTI, fromLabelInf, fromLabelRows, null, PropsUI.getInstance() );
    props.setLook( wFromLabelGrid );

    Button wGetFromLabel = new Button( wFromComp, SWT.PUSH );
    wGetFromLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wGetFromLabel.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        get( 0 );
      }
    } );
    FormData fdGetFromLabel = new FormData();
    fdGetFromLabel.right = new FormAttachment( 100, 0 );
    fdGetFromLabel.top = new FormAttachment( lastFromControl, margin );

    wGetFromLabel.setLayoutData( fdGetFromLabel );

    FormData fdFromLabelGrid = new FormData();
    fdFromLabelGrid.left = new FormAttachment( middle, margin );
    fdFromLabelGrid.top = new FormAttachment( lastFromControl, margin );
    fdFromLabelGrid.right = new FormAttachment( wGetFromLabel, 0 );
    fdFromLabelGrid.bottom = new FormAttachment( 0, margin * 2 + 150 );
    wFromLabelGrid.setLayoutData( fdFromLabelGrid );
    lastFromControl = wFromLabelGrid;


    // Node properties
    Label wlFromFields = new Label( wFromComp, SWT.RIGHT );
    wlFromFields.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromFields.Properties" ) );
    props.setLook( wlFromFields );
    FormData fdlFromFields = new FormData();
    fdlFromFields.left = new FormAttachment( 0, 0 );
    fdlFromFields.right = new FormAttachment( middle, 0 );
    fdlFromFields.top = new FormAttachment( lastFromControl, margin );
    wlFromFields.setLayoutData( fdlFromFields );
    final int fromPropsRows = ( input.getFromNodeProps() != null ? input.getFromNodeProps().length : 10 );
    ColumnInfo[] colinf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames,
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromFieldsTable.FromPropFieldsName" ), ColumnInfo.COLUMN_TYPE_TEXT, fieldNames,
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(),
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropPrimary" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" },
        false ),
    };
    wFromPropsGrid = new TableView( Variables.getADefaultVariableSpace(), wFromComp, SWT.BORDER
      | SWT.FULL_SELECTION | SWT.MULTI, colinf, fromPropsRows, null, props );
    props.setLook( wFromPropsGrid );

    Button wGetFromProps = new Button( wFromComp, SWT.PUSH );
    wGetFromProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wGetFromProps.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        get( 1 );
      }
    } );
    FormData fdGetFromProps = new FormData();
    fdGetFromProps.right = new FormAttachment( 100, 0 );
    fdGetFromProps.top = new FormAttachment( lastFromControl, margin );
    wGetFromProps.setLayoutData( fdGetFromProps );


    FormData fdFromPropsGrid = new FormData();
    fdFromPropsGrid.left = new FormAttachment( middle, margin );
    fdFromPropsGrid.right = new FormAttachment( wGetFromProps, 0 );
    fdFromPropsGrid.top = new FormAttachment( lastFromControl, margin );
    fdFromPropsGrid.bottom = new FormAttachment( 100, 0 );
    wFromPropsGrid.setLayoutData( fdFromPropsGrid );
    lastFromControl = wFromPropsGrid;


    FormData fdFromComp = new FormData();
    fdFromComp.left = new FormAttachment( 0, 0 );
    fdFromComp.top = new FormAttachment( 0, 0 );
    fdFromComp.right = new FormAttachment( 100, 0 );
    fdFromComp.bottom = new FormAttachment( 100, 0 );
    wFromComp.setLayoutData( fdFromComp );

    wFromComp.layout();
    wFromTab.setControl( wFromComp );



    /*
     * STRING_TO
     */

    CTabItem wToTab = new CTabItem( wTabFolder, SWT.NONE );
    wToTab.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToTab" ) ); //$NON-NLS-1$

    FormLayout toLayout = new FormLayout();
    toLayout.marginWidth = 3;
    toLayout.marginHeight = 3;

    Composite wToComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wToComp );
    wToComp.setLayout( toLayout );

    // Read only "to" node?
    //
    Label wlReadOnlyToNode = new Label( wToComp, SWT.RIGHT );
    wlReadOnlyToNode.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.ReadOnlyToNode" ) );
    props.setLook( wlReadOnlyToNode );
    FormData fdlReadOnlyToNodes = new FormData();
    fdlReadOnlyToNodes.left = new FormAttachment( 0, 0 );
    fdlReadOnlyToNodes.right = new FormAttachment( middle, 0 );
    fdlReadOnlyToNodes.top = new FormAttachment( 0, margin * 3 );
    wlReadOnlyToNode.setLayoutData( fdlReadOnlyToNodes );
    wReadOnlyToNode = new Button(wToComp, SWT.CHECK);
    props.setLook( wReadOnlyToNode );
    FormData fdReadOnlyToNode = new FormData();
    fdReadOnlyToNode.left = new FormAttachment( middle, margin );
    fdReadOnlyToNode.right = new FormAttachment( 100, 0 );
    fdReadOnlyToNode.top = new FormAttachment( 0, margin * 3 );
    wReadOnlyToNode.setLayoutData( fdReadOnlyToNode );
    Control lastToControl = wReadOnlyToNode;
    
    // Labels
    Label wlToLabel = new Label( wToComp, SWT.RIGHT );
    wlToLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.ToLabel" ) );
    props.setLook( wlToLabel );
    FormData fdlToLabels = new FormData();
    fdlToLabels.left = new FormAttachment( 0, 0 );
    fdlToLabels.right = new FormAttachment( middle, 0 );
    fdlToLabels.top = new FormAttachment( lastToControl, margin );
    wlToLabel.setLayoutData( fdlToLabels );
    final int toLabelRows = ( input.getToNodeLabels() != null ? input.getToNodeLabels().length : 10 );
    ColumnInfo[] toLabelInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToLabelsTable.ToFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToLabelsTable.ToValues" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };
    toLabelInf[1].setUsingVariables( true );

    wToLabelGrid =
      new TableView( Variables.getADefaultVariableSpace(), wToComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI, toLabelInf, toLabelRows, null, PropsUI.getInstance() );
    props.setLook( wToLabelGrid );

    Button wGetToLabel = new Button( wToComp, SWT.PUSH );
    wGetToLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wGetToLabel.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        get( 2 );
      }
    } );
    FormData fdGetToLabel = new FormData();
    fdGetToLabel.right = new FormAttachment( 100, 0 );
    fdGetToLabel.top = new FormAttachment( lastToControl, margin );
    wGetToLabel.setLayoutData( fdGetToLabel );

    FormData fdToLabelGrid = new FormData();
    fdToLabelGrid.left = new FormAttachment( middle, margin );
    fdToLabelGrid.right = new FormAttachment( wGetToLabel, 0 );
    fdToLabelGrid.top = new FormAttachment( lastToControl, margin );
    fdToLabelGrid.bottom = new FormAttachment( 0, margin * 2 + 150 );
    wToLabelGrid.setLayoutData( fdToLabelGrid );
    lastToControl = wToLabelGrid;

    // Node properties
    Label wlToFields = new Label( wToComp, SWT.RIGHT );
    wlToFields.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToFields.Properties" ) );
    props.setLook( wlToFields );
    FormData fdlToFields = new FormData();
    fdlToFields.left = new FormAttachment( 0, 0 );
    fdlToFields.right = new FormAttachment( middle, 0 );
    fdlToFields.top = new FormAttachment( lastToControl, margin );
    wlToFields.setLayoutData( fdlToFields );
    final int toPropsRows = ( input.getToNodeProps() != null ? input.getToNodeProps().length : 10 );
    ColumnInfo[] toColinf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToFieldsTable.ToFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToFieldsTable.ToFieldsName" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(),
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropPrimary" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "Y", "N" },
        false ),
    };

    wToPropsGrid =
      new TableView( Variables.getADefaultVariableSpace(), wToComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI, toColinf, toPropsRows, null, PropsUI.getInstance() );

    props.setLook( wToPropsGrid );


    Button wGetToProps = new Button( wToComp, SWT.PUSH );
    wGetToProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wGetToProps.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        get( 3 );
      }
    } );
    FormData fdGetToProps = new FormData();
    fdGetToProps.right = new FormAttachment( 100, 0 );
    fdGetToProps.top = new FormAttachment( lastToControl, margin );
    wGetToProps.setLayoutData( fdGetToProps );


    FormData fdToPropsGrid = new FormData();
    fdToPropsGrid.left = new FormAttachment( middle, margin );
    fdToPropsGrid.right = new FormAttachment( wGetToProps, 0 );
    fdToPropsGrid.top = new FormAttachment( lastToControl, margin );
    fdToPropsGrid.bottom = new FormAttachment( 100, 0 );
    wToPropsGrid.setLayoutData( fdToPropsGrid );


    FormData fdToComp = new FormData();
    fdToComp.left = new FormAttachment( 0, 0 );
    fdToComp.top = new FormAttachment( 0, 0 );
    fdToComp.right = new FormAttachment( 100, 0 );
    fdToComp.bottom = new FormAttachment( 100, 0 );
    wToComp.setLayoutData( fdToComp );

    wToComp.layout();
    wToTab.setControl( wToComp );

    /*
     * Relationships
     */
    CTabItem wRelationshipsTab = new CTabItem( wTabFolder, SWT.NONE );
    wRelationshipsTab.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelationshipsTab" ) ); //$NON-NLS-1$

    FormLayout relationshipsLayout = new FormLayout();
    relationshipsLayout.marginWidth = 3;
    relationshipsLayout.marginHeight = 3;

    Composite wRelationshipsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wRelationshipsComp );
    wRelationshipsComp.setLayout( relationshipsLayout );


    // Relationship field
    Label wlRel = new Label( wRelationshipsComp, SWT.RIGHT );
    wlRel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.Relationship.Label" ) );
    props.setLook( wlRel );
    FormData fdlRel = new FormData();
    fdlRel.left = new FormAttachment( 0, 0 );
    fdlRel.top = new FormAttachment( 0, 0 );
    wlRel.setLayoutData( fdlRel );
    wRel = new Combo( wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRel.setItems( fieldNames );
    props.setLook( wRel );
    wRel.addModifyListener( lsMod );
    FormData fdRel = new FormData();
    fdRel.left = new FormAttachment( wlRel, margin );
    fdRel.right = new FormAttachment( 100, 0);
    fdRel.top = new FormAttachment( wlRel, 0, SWT.CENTER );
    wRel.setLayoutData( fdRel );
    lastControl = wRel;

    // Relationship value field
    Label wlRelValue = new Label( wRelationshipsComp, SWT.RIGHT );
    wlRelValue.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelationshipValue.Label" ) );
    props.setLook( wlRelValue );
    FormData fdlRelValue = new FormData();
    fdlRelValue.left = new FormAttachment( 0, 0 );
    fdlRelValue.top = new FormAttachment( lastControl, margin*2  );
    wlRelValue.setLayoutData( fdlRelValue );
    wRelValue = new TextVar( transMeta, wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRelValue );
    wRelValue.addModifyListener( lsMod );
    FormData fdRelValue = new FormData();
    fdRelValue.left = new FormAttachment( wlRelValue, margin );
    fdRelValue.right = new FormAttachment( 100, 0);
    fdRelValue.top = new FormAttachment( wlRelValue, 0, SWT.CENTER );
    wRelValue.setLayoutData( fdRelValue );
    lastControl = wRelValue;

    // Relationship properties
    Label wlRelProps = new Label( wRelationshipsComp, SWT.RIGHT );
    wlRelProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelationshipProperties.Label" ) );
    props.setLook( wlRelProps );
    FormData fdlRelProps = new FormData();
    fdlRelProps.left = new FormAttachment( 0, 0 );
    fdlRelProps.top = new FormAttachment( lastControl, margin * 3 );
    wlRelProps.setLayoutData( fdlRelProps );

    final int relPropsRows = ( input.getRelProps() != null ? input.getRelProps().length : 10 );
    ColumnInfo[] relPropsInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesField" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames,
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesFieldName" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(),
        false ),
    };
    wRelPropsGrid = new TableView( Variables.getADefaultVariableSpace(), wRelationshipsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      relPropsInf, relPropsRows, null, PropsUI.getInstance() );
    props.setLook( wRelPropsGrid );

    Button wbRelProps = new Button( wRelationshipsComp, SWT.PUSH );
    wbRelProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wbRelProps.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        get( 4 );
      }
    } );
    FormData fdRelProps = new FormData();
    fdRelProps.right = new FormAttachment( 100, 0 );
    fdRelProps.top = new FormAttachment( lastControl, margin * 3);
    wbRelProps.setLayoutData( fdRelProps );


    FormData fdRelPropsGrid = new FormData();
    fdRelPropsGrid.left = new FormAttachment( wlRelProps, margin );
    fdRelPropsGrid.right = new FormAttachment( wbRelProps, -margin );
    fdRelPropsGrid.top = new FormAttachment( lastControl, margin * 3 );
    fdRelPropsGrid.bottom = new FormAttachment( 100, 0 );
    wRelPropsGrid.setLayoutData( fdRelPropsGrid );


    FormData fdRelationshipsComp = new FormData();
    fdRelationshipsComp.left = new FormAttachment( 0, 0 );
    fdRelationshipsComp.top = new FormAttachment( 0, 0 );
    fdRelationshipsComp.right = new FormAttachment( 100, 0 );
    fdRelationshipsComp.bottom = new FormAttachment( 100, 0 );
    wRelationshipsComp.setLayoutData( fdRelationshipsComp );

    wRelationshipsComp.layout();
    wRelationshipsTab.setControl( wRelationshipsComp );

    wTabFolder.setSelection( 0 );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void enableFields() {

    boolean toNeo = !wReturnGraph.getSelection();

    wlConnection.setEnabled( toNeo );
    wConnection.setEnabled( toNeo );
    wEditConnection.setEnabled( toNeo );
    wNewConnection.setEnabled( toNeo );
    wlBatchSize.setEnabled( toNeo );
    wBatchSize.setEnabled( toNeo );
    wlCreateIndexes.setEnabled( toNeo );
    wCreateIndexes.setEnabled( toNeo );
    wlUseCreate.setEnabled( toNeo );
    wUseCreate.setEnabled( toNeo );
    wlOnlyCreateRelationships.setEnabled( toNeo );
    wOnlyCreateRelationships.setEnabled( toNeo );

    wlReturnGraphField.setEnabled( !toNeo );
    wReturnGraphField.setEnabled( !toNeo );
  }

  private void getData() {
    wStepname.setText( stepname );
    wStepname.selectAll();
    wConnection.setText( Const.NVL( input.getConnection(), "" ) );
    wBatchSize.setText( Const.NVL( input.getBatchSize(), "" ) );
    wCreateIndexes.setSelection( input.isCreatingIndexes() );
    wUseCreate.setSelection( input.isUsingCreate() );
    wOnlyCreateRelationships.setSelection( input.isOnlyCreatingRelationships() );
    wReturnGraph.setSelection( input.isReturningGraph() );
    wReturnGraphField.setText(Const.NVL(input.getReturnGraphField(), ""));

    wReadOnlyFromNode.setSelection( input.isReadOnlyFromNode() );
    wReadOnlyToNode.setSelection( input.isReadOnlyToNode() );

    // List of connections...
    //
    try {
      List<String> elementNames = NeoConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Neo4j connections", e );
    }

    if ( input.getFromNodeLabels() != null ) {
      String fromNodeLabels[] = input.getFromNodeLabels();
      String fromNodeLabelValues[] = input.getFromNodeLabelValues();

      for ( int i = 0; i < fromNodeLabels.length; i++ ) {
        TableItem item = wFromLabelGrid.table.getItem( i );
        item.setText( 1, Const.NVL( fromNodeLabels[ i ], "" ) );
        item.setText( 2, Const.NVL( fromNodeLabelValues[ i ], "" ) );
      }
    }

    if ( input.getFromNodeProps() != null ) {

      for ( int i = 0; i < input.getFromNodeProps().length; i++ ) {
        TableItem item = wFromPropsGrid.table.getItem( i );
        item.setText( 1, Const.NVL( input.getFromNodeProps()[ i ], "" ) );
        item.setText( 2, Const.NVL( input.getFromNodePropNames()[ i ], "" ) );
        item.setText( 3, Const.NVL( input.getFromNodePropTypes()[ i ], "" ) );
        item.setText( 4, input.getFromNodePropPrimary()[ i ] ? "Y" : "N" );
      }
    }

    if ( input.getToNodeLabels() != null ) {
      String toNodeLabels[] = input.getToNodeLabels();
      String toNodeLabelValues[] = input.getToNodeLabelValues();

      for ( int i = 0; i < toNodeLabels.length; i++ ) {
        TableItem item = wToLabelGrid.table.getItem( i );
        item.setText( 1, Const.NVL( toNodeLabels[ i ], "" ) );
        item.setText( 2, Const.NVL( toNodeLabelValues[ i ], "" ) );
      }
    }

    if ( input.getToNodeProps() != null ) {
      for ( int i = 0; i < input.getToNodeProps().length; i++ ) {
        TableItem item = wToPropsGrid.table.getItem( i );
        item.setText( 1, Const.NVL( input.getToNodeProps()[ i ], "" ) );
        item.setText( 2, Const.NVL( input.getToNodePropNames()[ i ], "" ) );
        item.setText( 3, Const.NVL( input.getToNodePropTypes()[ i ], "" ) );
        item.setText( 4, input.getToNodePropPrimary()[ i ] ? "Y" : "N" );
      }
    }

    wRel.setText( Const.NVL( input.getRelationship(), "" ) );
    wRelValue.setText( Const.NVL( input.getRelationshipValue(), "" ) );

    if ( input.getRelProps() != null ) {
      for ( int i = 0; i < input.getRelProps().length; i++ ) {
        TableItem item = wRelPropsGrid.table.getItem( i );
        item.setText( 1, Const.NVL( input.getRelProps()[ i ], "" ) );
        item.setText( 2, Const.NVL( input.getRelPropNames()[ i ], "" ) );
        item.setText( 3, Const.NVL( input.getRelPropTypes()[ i ], "" ) );
      }
    }

    enableFields();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {

    stepname = wStepname.getText();
    input.setConnection( wConnection.getText() );
    input.setBatchSize( wBatchSize.getText() );
    input.setCreatingIndexes( wCreateIndexes.getSelection() );
    input.setUsingCreate( wUseCreate.getSelection() );
    input.setOnlyCreatingRelationships( wOnlyCreateRelationships.getSelection() );
    input.setReturningGraph( wReturnGraph.getSelection() );
    input.setReturnGraphField( wReturnGraphField.getText() );

    input.setReadOnlyFromNode( wReadOnlyFromNode.getSelection() );
    input.setReadOnlyToNode( wReadOnlyToNode.getSelection() );

    String fromNodeLabels[] = new String[ wFromLabelGrid.nrNonEmpty() ];
    String fromNodeLabelValues[] = new String[ wFromLabelGrid.nrNonEmpty() ];
    for ( int i = 0; i < fromNodeLabels.length; i++ ) {
      TableItem item = wFromLabelGrid.table.getItem( i );
      fromNodeLabels[ i ] = item.getText( 1 );
      fromNodeLabelValues[ i ] = item.getText( 2 );
    }
    input.setFromNodeLabels( fromNodeLabels );
    input.setFromNodeLabelValues( fromNodeLabelValues );

    String toNodeLabels[] = new String[ wToLabelGrid.nrNonEmpty() ];
    String toNodeLabelValues[] = new String[ wToLabelGrid.nrNonEmpty() ];
    for ( int i = 0; i < toNodeLabels.length; i++ ) {
      TableItem item = wToLabelGrid.table.getItem( i );
      toNodeLabels[ i ] = item.getText( 1 );
      toNodeLabelValues[ i ] = item.getText( 2 );
    }
    input.setToNodeLabels( toNodeLabels );
    input.setToNodeLabelValues( toNodeLabelValues );

    int nbFromPropLines = wFromPropsGrid.nrNonEmpty();
    String fromNodeProps[] = new String[ nbFromPropLines ];
    String fromNodePropNames[] = new String[ nbFromPropLines ];
    String fromNodePropTypes[] = new String[ nbFromPropLines ];
    boolean fromNodePropPrimary[] = new boolean[ nbFromPropLines ];

    for ( int i = 0; i < fromNodeProps.length; i++ ) {
      TableItem item = wFromPropsGrid.table.getItem( i );
      fromNodeProps[ i ] = item.getText( 1 );
      fromNodePropNames[ i ] = item.getText( 2 );
      fromNodePropTypes[ i ] = item.getText( 3 );
      fromNodePropPrimary[ i ] = "Y".equalsIgnoreCase( item.getText( 4 ) );
    }
    input.setFromNodeProps( fromNodeProps );
    input.setFromNodePropNames( fromNodePropNames );
    input.setFromNodePropTypes( fromNodePropTypes );
    input.setFromNodePropPrimary( fromNodePropPrimary );

    int nbToPropLines = wToPropsGrid.nrNonEmpty();
    String toNodeProps[] = new String[ nbToPropLines ];
    String toNodePropNames[] = new String[ nbToPropLines ];
    String toNodePropTypes[] = new String[ nbToPropLines ];
    boolean toNodePropPrimary[] = new boolean[ nbToPropLines ];

    for ( int i = 0; i < toNodeProps.length; i++ ) {
      TableItem item = wToPropsGrid.table.getItem( i );
      toNodeProps[ i ] = item.getText( 1 );
      toNodePropNames[ i ] = item.getText( 2 );
      toNodePropTypes[ i ] = item.getText( 3 );
      toNodePropPrimary[ i ] = "Y".equalsIgnoreCase( item.getText( 4 ) );
    }
    input.setToNodeProps( toNodeProps );
    input.setToNodePropNames( toNodePropNames );
    input.setToNodePropTypes( toNodePropTypes );
    input.setToNodePropPrimary( toNodePropPrimary );

    input.setRelationship( wRel.getText() );
    input.setRelationshipValue( wRelValue.getText() );

    int nbRelProps = wRelPropsGrid.nrNonEmpty();
    String relProps[] = new String[ nbRelProps ];
    String relPropNames[] = new String[ nbRelProps ];
    String relPropTypes[] = new String[ nbRelProps ];
    for ( int i = 0; i < relProps.length; i++ ) {
      TableItem item = wRelPropsGrid.table.getItem( i );
      relProps[ i ] = item.getText( 1 );
      relPropNames[ i ] = item.getText( 2 );
      relPropTypes[ i ] = item.getText( 3 );
    }
    input.setRelProps( relProps );
    input.setRelPropNames( relPropNames );
    input.setRelPropTypes( relPropTypes );

    // Mark step as changed
    stepMeta.setChanged();

    dispose();
  }


  private void get( int button ) {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        switch ( button ) {
          /* 0: from labels grid
           * 1: from properties grid
           * 2: to labels grid
           * 3: to properties grid
           * 4: relationship properties grid
           */
          case 0:
            BaseStepDialog.getFieldsFromPrevious( r, wFromLabelGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
            break;
          case 1:
            BaseStepDialog.getFieldsFromPrevious( r, wFromPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1,
              (item, valueMeta) -> getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4 )
            );
            break;
          case 2:
            BaseStepDialog.getFieldsFromPrevious( r, wToLabelGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
            break;
          case 3:
            BaseStepDialog.getFieldsFromPrevious( r, wToPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1,
              (item, valueMeta) -> getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4 )
            );
            break;
          case 4:
            BaseStepDialog.getFieldsFromPrevious( r, wRelPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1,
              (item, valueMeta) -> getPropertyNameTypePrimary(item, valueMeta, new int[] {2}, new int[] {3}, 4 )
            );
            break;
        }
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage" ), ke ); //$NON-NLS-1$ //$NON-NLS-2$
    }
  }

  private static final char[] delimitersLiteral = new char[] { ' ', '\t', ',', ';', '_', '-' };
  private static final String[] delimitersRegex = new String[] { "\\s", "\\t", ",", ";", "_", "-" };

  public static boolean getPropertyNameTypePrimary( TableItem item, ValueMetaInterface valueMeta, int[] nameColumns, int[] typeColumns, int primaryColumn ) {

    for (int nameColumn : nameColumns) {
      // Initcap the names in there, remove spaces and weird characters, lowercase first character
      // Issue #13
      //   Text Area 1 --> textArea1
      //   My_Silly_Column --> mySillyColumn
      //
      String propertyName = standardizePropertyName( valueMeta );

      item.setText( nameColumn, propertyName );
    }

    for (int typeColumn : typeColumns) {
      GraphPropertyType type = GraphPropertyType.getTypeFromKettle( valueMeta );
      item.setText(typeColumn, type.name());
    }

    if (primaryColumn>0) {
      item.setText( primaryColumn, "N" );
    }

    return true;
  }

  public static String standardizePropertyName( ValueMetaInterface valueMeta ) {
    String propertyName = valueMeta.getName();
    propertyName = WordUtils.capitalize( propertyName, delimitersLiteral );
    for (String delimiterRegex : delimitersRegex) {
      propertyName = propertyName.replaceAll( delimiterRegex, "");
    }
    if (propertyName.length()>0) {
      propertyName = propertyName.substring( 0, 1 ).toLowerCase() + propertyName.substring( 1 );
    }
    return propertyName;
  }

  private void newConnection() {
    NeoConnection connection = NeoConnectionUtils.newConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  private void editConnection() {
    NeoConnectionUtils.editConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ), wConnection.getText() );
  }
}
