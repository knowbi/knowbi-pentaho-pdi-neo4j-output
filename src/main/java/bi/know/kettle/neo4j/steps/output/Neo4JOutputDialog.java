package bi.know.kettle.neo4j.steps.output;


import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
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

  private CCombo wConnection;
  private TextVar wBatchSize;
  private Button wCreateIndexes;
  private Button wUseCreate;

  private Combo wRel;
  private TableView wFromPropsGrid;
  private TableView wFromLabelGrid;
  private TableView wToPropsGrid;
  private TableView wToLabelGrid;
  private TableView wRelPropsGrid;
  private String[] fieldNames;

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

    Label wlBatchSize = new Label( shell, SWT.RIGHT );
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

    Label wlCreateIndexes = new Label( shell, SWT.RIGHT );
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

    Label wlUseCreate = new Label( shell, SWT.RIGHT );
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

    // Labels
    Label wlFromLabel = new Label( wFromComp, SWT.RIGHT );
    wlFromLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.FromLabel" ) );
    props.setLook( wlFromLabel );
    FormData fdlFromLabels = new FormData();
    fdlFromLabels.left = new FormAttachment( 0, 0 );
    fdlFromLabels.top = new FormAttachment( 0, margin * 3 );
    wlFromLabel.setLayoutData( fdlFromLabels );
    final int fromLabelRows = ( input.getFromNodeLabels() != null ? input.getFromNodeLabels().length : 10 );
    ColumnInfo[] fromLabelInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromLabelsTable.FromFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
    };
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
    fdGetFromLabel.top = new FormAttachment( lastControl, margin * 3 );

    wGetFromLabel.setLayoutData( fdGetFromLabel );

    FormData fdFromLabelGrid = new FormData();
    fdFromLabelGrid.left = new FormAttachment( wlFromLabel, 0 );
    fdFromLabelGrid.top = new FormAttachment( 0, margin * 3 );
    fdFromLabelGrid.right = new FormAttachment( wGetFromLabel, 0 );
    fdFromLabelGrid.bottom = new FormAttachment( 0, margin * 2 + 150 );
    wFromLabelGrid.setLayoutData( fdFromLabelGrid );


    // Node properties
    Label wlFromFields = new Label( wFromComp, SWT.RIGHT );
    wlFromFields.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.FromFields.Properties" ) );
    props.setLook( wlFromFields );
    FormData fdlFromFields = new FormData();
    fdlFromFields.left = new FormAttachment( 0, 0 );
    fdlFromFields.top = new FormAttachment( wFromLabelGrid, margin );
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
    fdGetFromProps.top = new FormAttachment( wFromLabelGrid, margin );
    wGetFromProps.setLayoutData( fdGetFromProps );


    FormData fdFromPropsGrid = new FormData();
    fdFromPropsGrid.left = new FormAttachment( wlFromFields, margin );
    fdFromPropsGrid.right = new FormAttachment( wGetFromProps, 0 );
    fdFromPropsGrid.top = new FormAttachment( wFromLabelGrid, margin );
    fdFromPropsGrid.bottom = new FormAttachment( 100, 0 );
    wFromPropsGrid.setLayoutData( fdFromPropsGrid );


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


    // Labels
    Label wlToLabel = new Label( wToComp, SWT.RIGHT );
    wlToLabel.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.LabelsField.ToLabel" ) );
    props.setLook( wlToLabel );
    FormData fdlToLabels = new FormData();
    fdlToLabels.left = new FormAttachment( 0, 0 );
    fdlToLabels.top = new FormAttachment( 0, margin * 3 );
    wlToLabel.setLayoutData( fdlToLabels );
    final int toLabelRows = ( input.getToNodeLabels() != null ? input.getToNodeLabels().length : 10 );
    ColumnInfo[] toLabelInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToLabelsTable.ToFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
    };
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
    fdGetToLabel.top = new FormAttachment( 0, margin * 3 );
    wGetToLabel.setLayoutData( fdGetToLabel );

    FormData fdToLabelGrid = new FormData();
    fdToLabelGrid.left = new FormAttachment( wlToLabel, margin );
    fdToLabelGrid.right = new FormAttachment( wGetToLabel, 0 );
    fdToLabelGrid.top = new FormAttachment( 0, margin * 3 );
    fdToLabelGrid.bottom = new FormAttachment( 0, margin * 2 + 150 );
    wToLabelGrid.setLayoutData( fdToLabelGrid );

    // Node properties
    Label wlToFields = new Label( wToComp, SWT.RIGHT );
    wlToFields.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.ToFields.Properties" ) );
    props.setLook( wlToFields );
    FormData fdlToFields = new FormData();
    fdlToFields.left = new FormAttachment( 0, 0 );
    fdlToFields.top = new FormAttachment( wToLabelGrid, margin );
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
    fdGetToProps.top = new FormAttachment( wToLabelGrid, margin );
    wGetToProps.setLayoutData( fdGetToProps );


    FormData fdToPropsGrid = new FormData();
    fdToPropsGrid.left = new FormAttachment( wlToFields, margin );
    fdToPropsGrid.right = new FormAttachment( wGetToProps, 0 );
    fdToPropsGrid.top = new FormAttachment( wToLabelGrid, margin );
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
    fdlRel.top = new FormAttachment( lastControl, margin * 3 );
    wlRel.setLayoutData( fdlRel );

    wRel = new Combo( wRelationshipsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRel.setItems( fieldNames );
    props.setLook( wRel );
    wRel.addModifyListener( lsMod );
    FormData fdRel = new FormData();
    fdRel.left = new FormAttachment( wlRel, margin );
    fdRel.top = new FormAttachment( wlRel, 0, SWT.CENTER );
    wRel.setLayoutData( fdRel );


    // Relationship properties
    Label wlRelProps = new Label( wRelationshipsComp, SWT.RIGHT );
    wlRelProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelationshipField.Label" ) );
    props.setLook( wlRelProps );
    FormData fdlRelProps = new FormData();
    fdlRelProps.left = new FormAttachment( 0, 0 );
    fdlRelProps.top = new FormAttachment( wRel, margin * 3 );
    wlRelProps.setLayoutData( fdlRelProps );
    final int relPropsRows = ( input.getRelProps() != null ? input.getRelProps().length : 10 );
    ColumnInfo[] relPropsInf = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesField" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames,
        false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.RelPropsTable.PropertiesFieldName" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "Neo4JOutputDialog.PropType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(),
        false ),
    };
    wRelPropsGrid =
      new TableView( Variables.getADefaultVariableSpace(), wRelationshipsComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI, relPropsInf, relPropsRows, null, PropsUI.getInstance() );
    props.setLook( wRelPropsGrid );

    Button wRelProps = new Button( wRelationshipsComp, SWT.PUSH );
    wRelProps.setText( BaseMessages.getString( PKG, "Neo4JOutputDialog.GetFields.Button" ) ); //$NON-NLS-1$
    wRelProps.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent event ) {
        get( 4 );
      }
    } );
    FormData fdRelProps = new FormData();
    fdRelProps.right = new FormAttachment( 100, 0 );
    fdRelProps.top = new FormAttachment( wRel, margin * 3 );
    wRelProps.setLayoutData( fdRelProps );


    FormData fdRelPropsGrid = new FormData();
    fdRelPropsGrid.left = new FormAttachment( wlRelProps, margin );
    fdRelPropsGrid.right = new FormAttachment( wRelProps, 0 );
    fdRelPropsGrid.top = new FormAttachment( wRel, margin * 3 );
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

  private void getData() {
    wStepname.setText( stepname );
    wStepname.selectAll();
    wConnection.setText( Const.NVL( input.getConnection(), "" ) );
    wBatchSize.setText( Const.NVL( input.getBatchSize(), "" ) );
    wCreateIndexes.setSelection( input.isCreatingIndexes() );
    wUseCreate.setSelection( input.isUsingCreate() );

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

      for ( int i = 0; i < fromNodeLabels.length; i++ ) {
        TableItem item = wFromLabelGrid.table.getItem( i );
        item.setText( 1, Const.NVL( fromNodeLabels[ i ], "" ) );
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

      for ( int i = 0; i < toNodeLabels.length; i++ ) {
        TableItem item = wToLabelGrid.table.getItem( i );
        item.setText( 1, Const.NVL( toNodeLabels[ i ], "" ) );
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

    if ( input.getRelProps() != null ) {
      for ( int i = 0; i < input.getRelProps().length; i++ ) {
        TableItem item = wRelPropsGrid.table.getItem( i );
        item.setText( 1, Const.NVL( input.getRelProps()[ i ], "" ) );
        item.setText( 2, Const.NVL( input.getRelPropNames()[ i ], "" ) );
        item.setText( 3, Const.NVL( input.getRelPropTypes()[ i ], "" ) );
      }
    }
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

    String fromNodeLabels[] = new String[ wFromLabelGrid.nrNonEmpty() ];
    for ( int i = 0; i < fromNodeLabels.length; i++ ) {
      TableItem item = wFromLabelGrid.table.getItem( i );
      fromNodeLabels[ i ] = item.getText( 1 );
    }
    input.setFromNodeLabels( fromNodeLabels );

    String toNodeLabels[] = new String[ wToLabelGrid.nrNonEmpty() ];
    for ( int i = 0; i < toNodeLabels.length; i++ ) {
      TableItem item = wToLabelGrid.table.getItem( i );
      toNodeLabels[ i ] = item.getText( 1 );
    }
    input.setToNodeLabels( toNodeLabels );

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
            BaseStepDialog.getFieldsFromPrevious( r, wFromPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, this::getPropertyType );
            break;
          case 2:
            BaseStepDialog.getFieldsFromPrevious( r, wToLabelGrid, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
            break;
          case 3:
            BaseStepDialog.getFieldsFromPrevious( r, wToPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, this::getPropertyType );
            break;
          case 4:
            BaseStepDialog.getFieldsFromPrevious( r, wRelPropsGrid, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
            break;
        }
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage" ), ke ); //$NON-NLS-1$ //$NON-NLS-2$
    }
  }

  private boolean getPropertyType( TableItem item, ValueMetaInterface valueMeta ) {
    GraphPropertyType type = GraphPropertyType.getTypeFromKettle( valueMeta );
    item.setText( 3, type.name() );
    item.setText( 4, "N" );
    return true;
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
