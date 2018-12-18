package bi.know.kettle.neo4j.steps.dimensiongraph;

import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import org.apache.commons.lang.WordUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.*;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.Collections;
import java.util.List;

public class DimensionGraphDialog extends BaseStepDialog implements StepDialogInterface {
    private static Class<?> PKG = DimensionGraphDialog.class;

    private DimensionGraphMeta input;

    private boolean backupUpdate;
    private boolean gotPreviousFields;

    private String[] fieldNames;

    private Label wlUpdate, wlLabel, wlKey, wlProp, wlRelLabel, wlTkRename, wlVersion, wlDatefield;
    private Label wlFromdate, wlTodate, wlMinyear, wlMaxyear, wlUseAltStartDate;
    private Button wUpdate, wUseAltStartDate;
    private CCombo wConnection, wDatefield, wAltStartDate, wAltStartDateField;
    private TableView wNodeLabelGrid, wNodeKeyGrid, wNodePropGrid;
    private Text wTkRename, wVersion, wFromDate, wToDate, wMinyear, wMaxyear, wRelLabel;

    public DimensionGraphDialog(Shell parent, Object in, TransMeta transMeta, String sname ) {
        super( parent, (BaseStepMeta) in, transMeta, sname );
        input = (DimensionGraphMeta) in;
        metaStore = Spoon.getInstance().getMetaStore();
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();
        shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
        props.setLook( shell );
        setShellImage( shell, input );

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText( ModifyEvent e ) {
                input.setChanged();
            }
        };

        FocusListener lsConnectionFocus = new FocusAdapter() {
            public void focusLost( FocusEvent event ) {
                input.setChanged();
            }
        };

        ModifyListener lsTableMod = new ModifyListener() {
            public void modifyText( ModifyEvent arg0 ) {
                input.setChanged();
            }
        };

        backupChanged = input.hasChanged();
        backupUpdate = input.isUpdate();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
        shell.setLayout( formLayout );
        shell.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Shell.Title" ) );

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Fields
        try {
            RowMetaInterface prevFields = transMeta.getPrevStepFields( stepname );
            fieldNames = prevFields.getFieldNames();
        } catch ( KettleStepException kse ) {
            logError( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Log.ErrorGettingFieldNames" ) );
            fieldNames = new String[] {};
        }

        // dialog OK & Cancel buttons
        wOK = new Button( shell, SWT.PUSH );
        wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
        wCancel = new Button( shell, SWT.PUSH );
        wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$
        BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );
        // Add button listeners
        lsOK = e -> ok();
        lsCancel = e -> cancel();
        wOK.addListener( SWT.Selection, lsOK );
        wCancel.addListener( SWT.Selection, lsCancel );

        // Stepname line
        wlStepname = new Label( shell, SWT.RIGHT );
        wlStepname.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.StepName.Label" ) );
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

        // Update the dimension?
        wlUpdate = new Label( shell, SWT.RIGHT );
        wlUpdate.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Update.Label" ) );
        props.setLook( wlUpdate );
        FormData fdlUpdate = new FormData();
        fdlUpdate.left = new FormAttachment( 0, 0 );
        fdlUpdate.right = new FormAttachment( middle, 0 );
        fdlUpdate.top = new FormAttachment( wStepname, 2 * margin );
        wlUpdate.setLayoutData( fdlUpdate );
        wUpdate = new Button( shell, SWT.CHECK );
        props.setLook( wUpdate );
        FormData fdUpdate = new FormData();
        fdUpdate.left = new FormAttachment( middle, margin );
        fdUpdate.top = new FormAttachment( wlUpdate, 0, SWT.CENTER );
        fdUpdate.right = new FormAttachment( 100, 0 );
        wUpdate.setLayoutData( fdUpdate );

        // Clicking on update changes the options in the update combo boxes!
        wUpdate.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
                input.setUpdate( !input.isUpdate() );
                input.setChanged();
                setFlags();
            }
        } );

        // Neo4j Connection Line
        Label wlConnection = new Label( shell, SWT.RIGHT );
        wlConnection.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Connection.Label" ) );
        props.setLook( wlConnection );
        FormData fdlConnection = new FormData();
        fdlConnection.left = new FormAttachment( 0, 0 );
        fdlConnection.right = new FormAttachment( middle, 0 );
        fdlConnection.top = new FormAttachment( wUpdate, 2 * margin );
        wlConnection.setLayoutData( fdlConnection );

        Button wEditConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
        wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
        FormData fdEditConnection = new FormData();
        fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER);
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
        fdConnection.left = new FormAttachment( middle, margin );
        fdConnection.right = new FormAttachment( wNewConnection, 0 );
        fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
        wConnection.setLayoutData( fdConnection );

        // Commit Size Line

        // Enable the cache?

        // Preload the cache?

        // Cache Size in Rows (0 = cache all)


        // Tab Folder
        CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
        props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
        FormData fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment( 0, 0 );
        fdTabFolder.top = new FormAttachment( wConnection, 2 * margin );
        fdTabFolder.right = new FormAttachment( 100, 0 );
        fdTabFolder.bottom = new FormAttachment( 70, -margin );  // TODO: need adjust
        wTabFolder.setLayoutData( fdTabFolder );

        /////////////////////////
        // start of label tab
        /////////////////////////
        CTabItem wLabelTab = new CTabItem( wTabFolder, SWT.NONE );
        wLabelTab.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.LabelsTab" ) );

        FormLayout labelLayout = new FormLayout();
        labelLayout.marginWidth = 3;
        labelLayout.marginHeight = 3;

        Composite wLabelComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wLabelComp );
        wLabelComp.setLayout( labelLayout );

        wlLabel = new Label( wLabelComp, SWT.NONE );
        wlLabel.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.LabelFields.Label" ) );
        props.setLook( wlLabel );
        FormData fdlLable = new FormData();
        fdlLable.left = new FormAttachment( 0, 0 );
        fdlLable.top = new FormAttachment( 0, margin );
        fdlLable.right = new FormAttachment( 100, 0 );
        wlLabel.setLayoutData( fdlLable );

        final int nodeLabelRows = ( input.getDimensionNodeLabels() != null ? input.getDimensionNodeLabels().length : 10 );
        ColumnInfo[] nodeLabelInf = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodeLabelsTable.LabelFields" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodeLabelsTable.LabelValues" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
        };
        nodeLabelInf[1].setUsingVariables( true );
        wNodeLabelGrid = new TableView( Variables.getADefaultVariableSpace(), wLabelComp, SWT.BORDER
                | SWT.FULL_SELECTION | SWT.MULTI, nodeLabelInf, nodeLabelRows, null, PropsUI.getInstance() );
        props.setLook( wNodeLabelGrid );

        Button wGetNodeLabel = new Button( wLabelComp, SWT.PUSH );
        wGetNodeLabel.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.GetFields.Button" ) );
        wGetNodeLabel.addSelectionListener( new SelectionAdapter() {
            @Override
            public void widgetSelected( SelectionEvent arg0 ) {
                get( 0 );
            }
        } );
        FormData fdGetNodeLabel = new FormData();
        fdGetNodeLabel.top = new FormAttachment( wTabFolder, margin * 4 );
        fdGetNodeLabel.right = new FormAttachment( 100, 0 );
        wGetNodeLabel.setLayoutData( fdGetNodeLabel );

        FormData fdNodeLabelGrid = new FormData();
        fdNodeLabelGrid.left = new FormAttachment( 0, 0 );
        fdNodeLabelGrid.top = new FormAttachment( wlLabel, margin );
        fdNodeLabelGrid.right = new FormAttachment( wGetNodeLabel, 0 );
        fdNodeLabelGrid.bottom = new FormAttachment( 100, 0 );
        wNodeLabelGrid.setLayoutData( fdNodeLabelGrid );

        FormData fdLabelComp = new FormData();
        fdLabelComp.left = new FormAttachment( 0, 0 );
        fdLabelComp.top = new FormAttachment( 0, 0 );
        fdLabelComp.right = new FormAttachment( 100, 0 );
        fdLabelComp.bottom = new FormAttachment( 100, 0 );
        wLabelComp.setLayoutData( fdLabelComp );

        wLabelComp.layout();
        wLabelTab.setControl(wLabelComp);

        /////////////////////////
        // start of key tab
        /////////////////////////

        CTabItem wKeyTab = new CTabItem( wTabFolder, SWT.NONE );
        wKeyTab.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.KeysTab" ) );

        FormLayout keyLayout = new FormLayout();
        keyLayout.marginWidth = 3;
        keyLayout.marginHeight = 3;

        Composite wKeyComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wKeyComp );
        wKeyComp.setLayout( keyLayout );

        wlKey = new Label( wKeyComp, SWT.NONE );
        wlKey.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.KeyFields.Label" ) );
        props.setLook( wlKey );
        FormData fdlKey = new FormData();
        fdlKey.left = new FormAttachment( 0, 0 );
        fdlKey.top = new FormAttachment( 0, margin );
        fdlKey.right = new FormAttachment( 100, 0 );
        wlKey.setLayoutData( fdlKey );

        final int nodeKeyRows = ( input.getKeyFieldsStream() != null ? input.getKeyFieldsStream().length : 10 );
        ColumnInfo[] nodeKeyInf = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodeKeysTable.KeyFieldsStream" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodeKeysTable.KeyPropsLookup" ), ColumnInfo.COLUMN_TYPE_TEXT, fieldNames, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodeKeysTable.KeyPropsLookupType" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false ),
        };
        wNodeKeyGrid = new TableView( Variables.getADefaultVariableSpace(), wKeyComp, SWT.BORDER
                | SWT.FULL_SELECTION | SWT.MULTI, nodeKeyInf, nodeKeyRows, null, PropsUI.getInstance() );
        props.setLook( wNodeKeyGrid );

        Button wGetNodeKey = new Button( wKeyComp, SWT.PUSH );
        wGetNodeKey.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.GetFields.Button" ) );
        wGetNodeKey.addSelectionListener( new SelectionAdapter() {
            @Override
            public void widgetSelected( SelectionEvent arg0 ) {
                get( 1 );
            }
        } );
        FormData fdGetNodeKey = new FormData();
        fdGetNodeKey.top = new FormAttachment( wTabFolder, margin * 4 );
        fdGetNodeKey.right = new FormAttachment( 100, 0 );
        wGetNodeKey.setLayoutData( fdGetNodeKey );

        FormData fdNodeKeyGrid = new FormData();
        fdNodeKeyGrid.left = new FormAttachment( 0, 0 );
        fdNodeKeyGrid.top = new FormAttachment( wlKey, margin );
        fdNodeKeyGrid.right = new FormAttachment( wGetNodeKey, 0 );
        fdNodeKeyGrid.bottom = new FormAttachment( 100, 0 );
        wNodeKeyGrid.setLayoutData( fdNodeKeyGrid );

        FormData fdKeyComp = new FormData();
        fdKeyComp.left = new FormAttachment( 0, 0 );
        fdKeyComp.top = new FormAttachment( 0, 0 );
        fdKeyComp.right = new FormAttachment( 100, 0 );
        fdKeyComp.bottom = new FormAttachment( 100, 0 );
        wKeyComp.setLayoutData( fdKeyComp );

        wKeyComp.layout();
        wKeyTab.setControl(wKeyComp);

        /////////////////////////
        // start of property tab
        /////////////////////////
        CTabItem wPropTab = new CTabItem( wTabFolder, SWT.NONE );
        wPropTab.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.PropsTab" ) );

        FormLayout propLayout = new FormLayout();
        propLayout.marginWidth = 3;
        propLayout.marginHeight = 3;

        Composite wPropComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wPropComp );
        wPropComp.setLayout( propLayout );

        wlProp = new Label( wPropComp, SWT.NONE );
        wlProp.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.PropFields.Label" ) );
        props.setLook( wlProp );
        FormData fdlProp = new FormData();
        fdlProp.left = new FormAttachment( 0, 0 );
        fdlProp.top = new FormAttachment( 0, margin );
        fdlProp.right = new FormAttachment( 100, 0 );
        wlProp.setLayoutData( fdlProp );

        final int nodePropRows = ( input.getStreamFieldNames() != null ? input.getStreamFieldNames().length : 10 );
        ColumnInfo[] nodePropInf = new ColumnInfo[] {
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.StreamFieldNames" ), ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.NodePropNames" ), ColumnInfo.COLUMN_TYPE_TEXT, fieldNames, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.NodePropTypes" ), ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.NodePropPrimary" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{"Y","N"}, false ),
                new ColumnInfo( BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.NodePropUpdateType" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
                        input.isUpdate() ? DimensionGraphMeta.PropUpdateType.getNames() : DimensionGraphMeta.typeDescLookup ),
        };
        wNodePropGrid = new TableView( Variables.getADefaultVariableSpace(), wPropComp, SWT.BORDER
                | SWT.FULL_SELECTION | SWT.MULTI, nodePropInf, nodePropRows, null, PropsUI.getInstance() );
        props.setLook( wNodePropGrid );

        Button wGetNodeProp = new Button( wPropComp, SWT.PUSH );
        wGetNodeProp.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.GetFields.Button" ) );
        wGetNodeProp.addSelectionListener( new SelectionAdapter() {
            @Override
            public void widgetSelected( SelectionEvent arg0 ) {
                get( 2 );
            }
        } );
        FormData fdGetNodeProp = new FormData();
        fdGetNodeProp.top = new FormAttachment( wTabFolder, margin * 4 );
        fdGetNodeProp.right = new FormAttachment( 100, 0 );
        wGetNodeProp.setLayoutData( fdGetNodeProp );

        FormData fdNodePropGrid = new FormData();
        fdNodePropGrid.left = new FormAttachment( 0, 0 );
        fdNodePropGrid.top = new FormAttachment( wlProp, margin );
        fdNodePropGrid.right = new FormAttachment( wGetNodeProp, 0 );
        fdNodePropGrid.bottom = new FormAttachment( 100, 0 );
        wNodePropGrid.setLayoutData( fdNodePropGrid );

        FormData fdPropComp = new FormData();
        fdPropComp.left = new FormAttachment( 0, 0 );
        fdPropComp.top = new FormAttachment( 0, 0 );
        fdPropComp.right = new FormAttachment( 100, 0 );
        fdPropComp.bottom = new FormAttachment( 100, 0 );
        wPropComp.setLayoutData( fdPropComp );

        wPropComp.layout();
        wPropTab.setControl(wPropComp);
        wTabFolder.setSelection( 0 );

        // Dimension Relationship label line
        wlRelLabel = new Label( shell, SWT.RIGHT );
        wlRelLabel.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.RelLabel.Label" ) );
        props.setLook( wlRelLabel );
        FormData fdlRelLabel = new FormData();
        fdlRelLabel.left = new FormAttachment( 0, 0 );
        fdlRelLabel.right = new FormAttachment( middle, 0 );
        fdlRelLabel.top = new FormAttachment( 70, 2*margin );
        wlRelLabel.setLayoutData( fdlRelLabel );
        wRelLabel = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wRelLabel );
        wRelLabel.addModifyListener( lsMod );
        FormData fdRelLabel = new FormData();
        fdRelLabel.left = new FormAttachment( middle, margin );
        fdRelLabel.top = new FormAttachment( wlRelLabel, 0, SWT.CENTER );
        fdRelLabel.right = new FormAttachment( 100, 0 );
        wRelLabel.setLayoutData( fdRelLabel );


        // Technical Key Line
        wlTkRename = new Label( shell, SWT.RIGHT );
        wlTkRename.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.TKNewName.Label" ) );
        props.setLook( wlTkRename );
        FormData fdlTkRename = new FormData();
        fdlTkRename.left = new FormAttachment( 0, 0 );
        fdlTkRename.right = new FormAttachment( middle, 0 );
        fdlTkRename.top = new FormAttachment( wRelLabel, 2 * margin );
        wlTkRename.setLayoutData( fdlTkRename );
        wTkRename = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wTkRename );
        wTkRename.addModifyListener( lsMod );
        FormData fdTkRename = new FormData();
        fdTkRename.left = new FormAttachment( middle, margin );
        fdTkRename.top = new FormAttachment( wlTkRename, 0, SWT.CENTER );
        fdTkRename.right = new FormAttachment( 100, 0 );
        wTkRename.setLayoutData( fdTkRename );


        // Version property Line
        wlVersion = new Label( shell, SWT.RIGHT );
        wlVersion.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Version.Label" ) );
        props.setLook( wlVersion );
        FormData fdlVersion = new FormData();
        fdlVersion.left = new FormAttachment( 0, 0 );
        fdlVersion.right = new FormAttachment( middle, 0 );
        fdlVersion.top = new FormAttachment( wTkRename, 2 * margin );
        wlVersion.setLayoutData( fdlVersion );
        wVersion = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wVersion );
        wVersion.addModifyListener( lsMod );
        FormData fdVersion = new FormData();
        fdVersion.left = new FormAttachment( middle,  margin);
        fdVersion.top = new FormAttachment( wlVersion, 0, SWT.CENTER );
        fdVersion.right = new FormAttachment( 100, 0 );
        wVersion.setLayoutData( fdVersion );


        //Stream date field line
        wlDatefield = new Label( shell, SWT.RIGHT );
        wlDatefield.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Datefield.Label" ) );
        props.setLook( wlDatefield );
        FormData fdlDatefield = new FormData();
        fdlDatefield.left = new FormAttachment( 0, 0 );
        fdlDatefield.right = new FormAttachment( middle, 0 );
        fdlDatefield.top = new FormAttachment( wVersion, 2 * margin );
        wlDatefield.setLayoutData( fdlDatefield );
        wDatefield = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wDatefield );
        wDatefield.addModifyListener( lsMod );
        FormData fdDatefield = new FormData();
        fdDatefield.left = new FormAttachment( middle, margin );
        fdDatefield.top = new FormAttachment( wlDatefield, 0, SWT.CENTER );
        fdDatefield.right = new FormAttachment( 100, 0 );
        wDatefield.setLayoutData( fdDatefield );
        wDatefield.addFocusListener( new FocusListener() {
            public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
            }

            public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
                Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
                shell.setCursor( busy );
                getFields();
                shell.setCursor( null );
                busy.dispose();
            }
        } );

        // From date line
        //
        wlFromdate = new Label( shell, SWT.RIGHT );
        wlFromdate.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Fromdate.Label" ) );
        props.setLook( wlFromdate );
        FormData fdlFromdate = new FormData();
        fdlFromdate.left = new FormAttachment( 0, 0 );
        fdlFromdate.right = new FormAttachment( middle, 0 );
        fdlFromdate.top = new FormAttachment( wDatefield, 2 * margin );
        wlFromdate.setLayoutData( fdlFromdate );
        wFromDate = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook(wFromDate);
        wFromDate.addModifyListener( lsMod );
        FormData fdUpdateDate = new FormData();
        fdUpdateDate.left = new FormAttachment( middle, margin );
        fdUpdateDate.right = new FormAttachment( middle + ( 100 - middle ) / 3, -margin );
        fdUpdateDate.top = new FormAttachment( wlFromdate, 0, SWT.CENTER );
        wFromDate.setLayoutData( fdUpdateDate );

        // Minyear line
        wlMinyear = new Label( shell, SWT.RIGHT );
        wlMinyear.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Minyear.Label" ) );
        props.setLook( wlMinyear );
        FormData fdlMinyear = new FormData();
        fdlMinyear.left = new FormAttachment(wFromDate, margin );
        fdlMinyear.right = new FormAttachment( middle + 2 * ( 100 - middle ) / 3, -margin );
        fdlMinyear.top = new FormAttachment( wlFromdate, 0, SWT.CENTER );
        wlMinyear.setLayoutData( fdlMinyear );
        wMinyear = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wMinyear );
        wMinyear.addModifyListener( lsMod );
        FormData fdMinyear = new FormData();
        fdMinyear.left = new FormAttachment( wlMinyear, margin );
        fdMinyear.right = new FormAttachment( 100, 0 );
        fdMinyear.top = new FormAttachment( wlFromdate, 0, SWT.CENTER );
        wMinyear.setLayoutData( fdMinyear );
        wMinyear.setToolTipText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Minyear.ToolTip" ) );

        // Add a line with an option to specify an alternative start date...
        //
        wlUseAltStartDate = new Label( shell, SWT.RIGHT );
        wlUseAltStartDate.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.UseAlternativeStartDate.Label" ) );
        props.setLook( wlUseAltStartDate );
        FormData fdlUseAltStartDate = new FormData();
        fdlUseAltStartDate.left = new FormAttachment( 0, 0 );
        fdlUseAltStartDate.right = new FormAttachment( middle, 0 );
        fdlUseAltStartDate.top = new FormAttachment( wMinyear, 2 * margin );
//        fdlUseAltStartDate.bottom = new FormAttachment( 100, 0 );
        wlUseAltStartDate.setLayoutData( fdlUseAltStartDate );
        wUseAltStartDate = new Button( shell, SWT.CHECK );
        props.setLook( wUseAltStartDate );
        wUseAltStartDate.setToolTipText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.UseAlternativeStartDate.Tooltip", Const.CR ) );
        FormData fdUseAltStartDate = new FormData();
        fdUseAltStartDate.left = new FormAttachment( middle, margin );
        fdUseAltStartDate.top = new FormAttachment( wlUseAltStartDate, 0, SWT.CENTER );
//        fdUseAltStartDate.bottom = new FormAttachment( 100, 0 );
        wUseAltStartDate.setLayoutData( fdUseAltStartDate );
        wUseAltStartDate.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
                setFlags();
                input.setChanged();
            }
        } );

        // The choice...
        //
        wAltStartDate = new CCombo( shell, SWT.BORDER );
        props.setLook( wAltStartDate );
        // All options except for "No alternative"...
        wAltStartDate.removeAll();
        for ( int i = 1; i < DimensionGraphMeta.StartDateAlternativeType.values().length; i++ ) {
            wAltStartDate.add( DimensionGraphMeta.StartDateAlternativeType.values()[i].name() );
        }
        wAltStartDate.setText( BaseMessages.getString(PKG, "Neo4jDimensionGraphDialog.AlternativeStartDate.SelectItemDefault" ) );
        wAltStartDate.setToolTipText( BaseMessages.getString(PKG, "Neo4jDimensionGraphDialog.AlternativeStartDate.Tooltip", Const.CR ) );
        FormData fdAltStartDate = new FormData();
        fdAltStartDate.left = new FormAttachment( wUseAltStartDate, margin );
        fdAltStartDate.right = new FormAttachment( wUseAltStartDate, 200 );
        fdAltStartDate.top = new FormAttachment( wlUseAltStartDate, 0, SWT.CENTER );
//        fdAltStartDate.bottom = new FormAttachment( 100, 0 );
        wAltStartDate.setLayoutData( fdAltStartDate );
        wAltStartDate.addModifyListener( new ModifyListener() {
            public void modifyText( ModifyEvent arg0 ) {
                setFlags();
                input.setChanged();
            }
        } );
        wAltStartDateField = new CCombo( shell, SWT.SINGLE | SWT.BORDER );
        props.setLook( wAltStartDateField );
        wAltStartDateField.setToolTipText( BaseMessages.getString(PKG, "Neo4jDimensionGraphDialog.AlternativeStartDateField.Tooltip", Const.CR ) );
        FormData fdAltStartDateField = new FormData();
        fdAltStartDateField.left = new FormAttachment( wAltStartDate, margin );
        fdAltStartDateField.right = new FormAttachment( 100, 0 );
        fdAltStartDateField.top = new FormAttachment( wlUseAltStartDate, 0, SWT.CENTER );
        wAltStartDateField.setLayoutData( fdAltStartDateField );
        wAltStartDateField.addFocusListener( new FocusListener() {
            public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
            }

            public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
                Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
                shell.setCursor( busy );
                getFields();
                shell.setCursor( null );
                busy.dispose();
            }
        } );

        // To date line
        //
        wlTodate = new Label( shell, SWT.RIGHT );
        wlTodate.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Todate.Label" ) );
        props.setLook( wlTodate );
        FormData fdlTodate = new FormData();
        fdlTodate.left = new FormAttachment( 0, 0 );
        fdlTodate.right = new FormAttachment( middle, 0 );
        fdlTodate.top = new FormAttachment( wAltStartDateField, 2 * margin );
        wlTodate.setLayoutData( fdlTodate );
        wToDate = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wToDate );
        wToDate.addModifyListener( lsMod );
        FormData fdToDate = new FormData();
        fdToDate.left = new FormAttachment( middle, margin );
        fdToDate.right = new FormAttachment( middle + ( 100 - middle ) / 3, -margin );
        fdToDate.top = new FormAttachment( wlTodate, 0, SWT.CENTER );
        wToDate.setLayoutData( fdToDate );

        // Maxyear line
        wlMaxyear = new Label( shell, SWT.RIGHT );
        wlMaxyear.setText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Maxyear.Label" ) );
        props.setLook( wlMaxyear );
        FormData fdlMaxyear = new FormData();
        fdlMaxyear.left = new FormAttachment( wToDate, margin );
        fdlMaxyear.right = new FormAttachment( middle + 2 * ( 100 - middle ) / 3, -margin );
        fdlMaxyear.top = new FormAttachment( wlTodate, 0, SWT.CENTER );
        wlMaxyear.setLayoutData( fdlMaxyear );
        wMaxyear = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        props.setLook( wMaxyear );
        wMaxyear.addModifyListener( lsMod );
        FormData fdMaxyear = new FormData();
        fdMaxyear.left = new FormAttachment( wlMaxyear, margin );
        fdMaxyear.right = new FormAttachment( 100, 0 );
        fdMaxyear.top = new FormAttachment( wlTodate, 0, SWT.CENTER );
        wMaxyear.setLayoutData( fdMaxyear );
        wMaxyear.setToolTipText( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Maxyear.ToolTip" ) );


        ///////////////

        lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected( SelectionEvent e ) {
                ok();
            }
        };

        wStepname.addSelectionListener( lsDef );
        wRelLabel.addSelectionListener( lsDef );
        wTkRename.addSelectionListener( lsDef );
        wVersion.addSelectionListener( lsDef );
        wDatefield.addSelectionListener( lsDef );
        wFromDate.addSelectionListener( lsDef );
        wMinyear.addSelectionListener( lsDef );
        wMaxyear.addSelectionListener( lsDef );

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

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    private void getData() {
        if ( log.isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.Log.GettingKeyInfo" ) );
        }

        wStepname.setText( stepname );
        wStepname.selectAll();
        wUpdate.setSelection( input.isUpdate() );
        wConnection.setText( Const.NVL( input.getConnection(), "" ) );
        // List of connections...
        //
        try {
            List<String> elementNames = NeoConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
            Collections.sort( elementNames );
            wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
        } catch ( Exception e ) {
            new ErrorDialog( shell, "Error", "Unable to list Neo4j connections", e );
        }

        // node labels load
        if ( input.getDimensionNodeLabels() != null ) {
            String dimensionNodeLabels[] = input.getDimensionNodeLabels();
            String dimensionNodeLabelValues[] = input.getDimensionNodeLabelValues();
            for ( int i = 0; i < dimensionNodeLabels.length; i++ ) {
                TableItem item = wNodeLabelGrid.table.getItem( i );
                item.setText( 1, Const.NVL( dimensionNodeLabels[ i ], "" ) );
                item.setText( 2, Const.NVL( dimensionNodeLabelValues[ i ], "" ) );
            }
        }

        // Keys load
        if ( input.getKeyFieldsStream() != null ) {
            String keyFieldsStream[] = input.getKeyFieldsStream();
            String keyPropsLookup[] = input.getKeyPropsLookup();
            String keyPropsLookupType[] = input.getKeyPropsLookupType();
            for ( int i = 0; i < keyFieldsStream.length; i++ ) {
                TableItem item = wNodeKeyGrid.table.getItem( i );
                item.setText( 1, Const.NVL( keyFieldsStream[ i ], "" ) );
                item.setText( 2, Const.NVL( keyPropsLookup[ i ], "" ) );
                item.setText( 3, Const.NVL( keyPropsLookupType[ i ], "" ) );
            }
        }

        // Props load
        if ( input.getStreamFieldNames() != null ) {
            String streamFieldNames[] = input.getStreamFieldNames();
            String nodePropNames[] = input.getNodePropNames();
            String nodePropTypes[] = input.getNodePropTypes();
            for ( int i = 0; i < streamFieldNames.length; i++ ) {
                TableItem item = wNodePropGrid.table.getItem( i );
                item.setText( 1, Const.NVL( streamFieldNames[ i ], "" ) );
                item.setText( 2, Const.NVL( nodePropNames[ i ], "" ) );
                item.setText( 3, Const.NVL( nodePropTypes[ i ], "" ) );
                item.setText( 4, input.getNodePropPrimary()[ i ] ? "Y" : "N" );
                item.setText( 5, input.isUpdate() ? input.getNodePropUpdateType()[ i ].name()
                        : ValueMetaFactory.getValueMetaName(input.getReturnType()[ i ] ) );
            }
        }

        if( input.getRelLabelValue() != null ) {
            wRelLabel.setText(input.getRelLabelValue());
        }
        if (input.getIdRename() != null) {
            wTkRename.setText(input.getIdRename());
        }
        if (input.getVersionProp() != null) {
            wVersion.setText(input.getVersionProp());
        }
        if ( input.getStreamDateField() != null ) {
            wDatefield.setText( input.getStreamDateField() );
        }
        if (input.getStartDateProp() != null) {
            wFromDate.setText( Const.NVL( input.getStartDateProp(), "" ) );
        }
        wMinyear.setText( "" + input.getMinYear() );
        if (input.getEndDateProp() != null) {
            wToDate.setText( Const.NVL( input.getEndDateProp(), "" ) );
        }
        wMaxyear.setText( "" + input.getMaxYear() );
        wUseAltStartDate.setSelection( input.isUsingStartDateAlternative() );
        if( input.isUsingStartDateAlternative() ) {
            wAltStartDate.setText( input.getStartDateAlternativeType().name() );
        }
        if (input.getStartDateFieldName() != null) {
            wAltStartDateField.setText( Const.NVL( input.getStartDateFieldName(), "" ) );
        }

        wNodeLabelGrid.removeEmptyRows();
        wNodeLabelGrid.setRowNums();
        wNodeLabelGrid.optWidth( true );
        wNodeKeyGrid.removeEmptyRows();
        wNodeKeyGrid.setRowNums();
        wNodeKeyGrid.optWidth( true );
        wNodePropGrid.removeEmptyRows();
        wNodePropGrid.setRowNums();
        wNodePropGrid.optWidth( true );

        setFlags();

        wStepname.selectAll();
        wStepname.setFocus();
    }

    public void setFlags() {
        ColumnInfo colinf = new ColumnInfo(
                        BaseMessages.getString( PKG, "Neo4jDimensionGraph.NodePropsTable.NodePropUpdateType" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
                        input.isUpdate() ? DimensionGraphMeta.PropUpdateType.getNames() : DimensionGraphMeta.typeDescLookup );
        wNodePropGrid.setColumnInfo( 4, colinf );

        if ( input.isUpdate() ) {
            wNodePropGrid.setColumnText( 1, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnText.StreamFieldToCompare" ) );
            wNodePropGrid.setColumnText( 5, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnText.TypeOfDimensionUpdate" ) );
            wNodePropGrid.setColumnToolTip( 1, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnToolTip" )
                    + Const.CR + "Punch Through: Kimball Type I" + Const.CR + "Update: Correct error in last version" );
        } else {
            wNodePropGrid.setColumnText( 1, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnText.NewNameOfOutputField" ) );
            wNodePropGrid.setColumnText( 5, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnText.TypeOfReturnField" ) );
            wNodePropGrid.setColumnToolTip( 1, BaseMessages.getString(
                    PKG, "Neo4jDimensionGraphDialog.UpdateOrInsertFields.ColumnToolTip2" ) );
        }

        if ( input.getStreamFieldNames() != null ) {
            String streamFieldNames[] = input.getStreamFieldNames();
            String nodePropNames[] = input.getNodePropNames();
            String nodePropTypes[] = input.getNodePropTypes();

            for ( int i = 0; i < wNodePropGrid.nrNonEmpty(); i++ ) {
                    TableItem item = wNodePropGrid.table.getItem(i);
                try {
                    item.setText(5, input.isUpdate() ? input.getNodePropUpdateType()[i].name()
                            : ValueMetaFactory.getValueMetaName(input.getReturnType()[i]));
                } catch (ArrayIndexOutOfBoundsException e ) {
                    item.setText(5, input.isUpdate() ? DimensionGraphMeta.PropUpdateType.INSERT.name()
                            : ValueMetaFactory.getValueMetaName( ValueMetaInterface.TYPE_STRING ));
                }
            }
        }

        wNodePropGrid.optWidth( true );

        // In case of lookup: disable commitsize, etc.
        boolean update = wUpdate.getSelection();
        wlMinyear.setEnabled( update );
        wMinyear.setEnabled( update );
        wlMaxyear.setEnabled( update );
        wMaxyear.setEnabled( update );
        wlVersion.setEnabled( update );
        wVersion.setEnabled( update );

        // The alternative start date
        //
        wAltStartDate.setEnabled( wUseAltStartDate.getSelection() );
        DimensionGraphMeta.StartDateAlternativeType alternative = DimensionGraphMeta.getStartDateAlternativeType( wAltStartDate.getText() );
        wAltStartDateField.setEnabled( alternative == DimensionGraphMeta.StartDateAlternativeType.FIELD_VALUE);
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

    private void get( int button ) {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields( stepname );
            if( r != null && !r.isEmpty() ) {
                switch ( button ) {
                    case 0:
                        BaseStepDialog.getFieldsFromPrevious( r, wNodeLabelGrid, 1, new int[]{1}, new int[]{}, -1, -1, null );
                        break;
                    case 1:
                        BaseStepDialog.getFieldsFromPrevious( r, wNodeKeyGrid, 1, new int[]{1,2}, new int[]{}, -1, -1,
                                (item, valueMeta) -> getPropertyNameTypePrimary( item, valueMeta, new int[]{2}, new int[]{3}, -1, -1, input.isUpdate()) );
                        break;
                    case 2:
                        BaseStepDialog.getFieldsFromPrevious( r, wNodePropGrid, 1, new int[]{1,2}, new int[]{}, -1, -1,
                                (item, valueMeta) -> getPropertyNameTypePrimary( item, valueMeta, new int[]{2}, new int[]{3}, 4, 5, input.isUpdate()) );
                        break;
                }
            }
        } catch ( KettleException ke ) {
            new ErrorDialog( shell, BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle" ),
                    BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage" ), ke );
        }
    }

    public static boolean getPropertyNameTypePrimary(TableItem item, ValueMetaInterface valueMeta,
                                                     int[] nameColumns, int[] typeColumns, int primaryColumn, int returnTypeColumn, boolean isUpdate) {
        for (int nameColumn : nameColumns) {
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

        if (returnTypeColumn>0 & !isUpdate) {
                item.setText(returnTypeColumn, valueMeta.getTypeDesc());
        }

        return true;
    }

    private static final char[] delimitersLiteral = new char[] { ' ', '\t', ',', ';', '_', '-' };
    private static final String[] delimitersRegex = new String[] { "\\s", "\\t", ",", ";", "_", "-" };

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



    private void getFields() {
        if ( !gotPreviousFields ) {
            try {
                String field = wDatefield.getText();
                RowMetaInterface r = transMeta.getPrevStepFields( stepname );
                if ( r != null ) {
                    wDatefield.setItems( r.getFieldNames() );

                }
                if ( field != null ) {
                    wDatefield.setText( field );
                }
            } catch ( KettleException ke ) {
                new ErrorDialog(
                        shell, BaseMessages.getString( PKG, "Neo4jDimensionGraphDialog.ErrorGettingFields.Title" ), BaseMessages
                        .getString( PKG, "Neo4jDimensionGraphDialog.ErrorGettingFields.Message" ), ke );
            }
            gotPreviousFields = true;
        }
    }

    public void getInfo( DimensionGraphMeta input ) {
        stepname = wStepname.getText(); // return value
        input.setConnection( wConnection.getText() );
        input.setUpdate( wUpdate.getSelection() );

        String nodeLabels[] = new String[ wNodeLabelGrid.nrNonEmpty() ];
        String nodeLabelValues[] = new String[ wNodeLabelGrid.nrNonEmpty() ];
        for ( int i = 0; i < nodeLabels.length; i++ ) {
            TableItem item = wNodeLabelGrid.table.getItem( i );
            nodeLabels[ i ] = item.getText( 1 );
            nodeLabelValues[ i ] = item.getText( 2 );
        }
        input.setDimensionNodeLabels( nodeLabels );
        input.setDimensionNodeLabelValues( nodeLabelValues );

        String keysStream[] = new String[ wNodeKeyGrid.nrNonEmpty() ];
        String keyProps[] = new String[ wNodeKeyGrid.nrNonEmpty() ];
        String keyTypes[] = new String[ wNodeKeyGrid.nrNonEmpty() ];
        for ( int i = 0; i < keysStream.length; i++ ) {
            TableItem item = wNodeKeyGrid.table.getItem( i );
            keysStream[ i ] = item.getText( 1 );
            keyProps[ i ] = item.getText( 2 );
            keyTypes[ i ] = item.getText( 3 );
        }
        input.setKeyFieldsStream( keysStream );
        input.setKeyPropsLookup( keyProps );
        input.setKeyPropsLookupType( keyTypes );

        String fieldsStream[] = new String[ wNodePropGrid.nrNonEmpty() ];
        String propsName[] = new String[ wNodePropGrid.nrNonEmpty() ];
        String propsType[] = new String[ wNodePropGrid.nrNonEmpty() ];
        boolean propsPrimary[] = new boolean[ wNodePropGrid.nrNonEmpty() ];
        DimensionGraphMeta.PropUpdateType propsUpdate[] = new DimensionGraphMeta.PropUpdateType[ wNodePropGrid.nrNonEmpty() ];
        int propsReturnType[] = new int[ wNodePropGrid.nrNonEmpty() ];
        for ( int i = 0; i < fieldsStream.length; i++ ) {
            TableItem item = wNodePropGrid.table.getItem( i );
            fieldsStream[ i ] = item.getText( 1 );
            propsName[ i ] = item.getText( 2 );
            propsType[ i ] = item.getText( 3 );
            propsPrimary[ i ] = item.getText( 4).equalsIgnoreCase("Y")? true : false;
            if(input.isUpdate()) {
                propsUpdate[ i ] = DimensionGraphMeta.getPropUpdateType( item.getText( 5 ) );
            } else {
                propsReturnType[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 5 ) );
                if ( propsReturnType[ i ] == ValueMetaInterface.TYPE_NONE ) {
                    propsReturnType[ i ] = ValueMetaInterface.TYPE_STRING;
                }
            }
        }
        input.setStreamFieldNames( fieldsStream );
        input.setNodePropNames( propsName );
        input.setNodePropTypes( propsType );
        input.setNodePropPrimary( propsPrimary );
        if( input.isUpdate() ) {
            input.setNodePropUpdateType( propsUpdate );
        } else {
            input.setReturnType( propsReturnType );
        }

        input.setRelLabelValue( wRelLabel.getText() );
        input.setIdRename( wTkRename.getText() );
        input.setVersionProp( wVersion.getText() );
        input.setStreamDateField( wDatefield.getText() );

        input.setStartDateProp( wFromDate.getText() );
        input.setMinYear( Const.toInt( wMinyear.getText(), Const.MIN_YEAR ) );
        input.setEndDateProp( wToDate.getText() );
        input.setMaxYear( Const.toInt( wMaxyear.getText(), Const.MAX_YEAR ) );
        input.setUsingStartDateAlternative( wUseAltStartDate.getSelection() );
        input.setStartDateAlternativeType( DimensionGraphMeta.getStartDateAlternativeType( wAltStartDate.getText() ) );
        if( wAltStartDate.getText().equalsIgnoreCase( DimensionGraphMeta.StartDateAlternativeType.FIELD_VALUE.name() ) ) {
            input.setStartDateFieldName( wAltStartDateField.getText() );
        }
    }

    private void ok() {
        if ( Utils.isEmpty( wStepname.getText()) ) {
            return;
        }
        getInfo( input );
        dispose();
    }

    private void cancel() {
        stepname = null;
        input.setChanged( changed );
        dispose();
    }
}
