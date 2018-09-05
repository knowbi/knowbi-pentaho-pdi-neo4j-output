package bi.know.kettle.neo4j.steps.cypher;

import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import bi.know.kettle.neo4j.steps.output.Neo4JOutputDialog;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.csvinput.CsvInputMeta;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CypherDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = CypherMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;

  private CCombo wConnection;

  private TextVar wBatchSize;

  private Button wCypherFromField;
  private CCombo wCypherField;
  private Button wUnwind;
  private Label wlUnwindMap;
  private TextVar wUnwindMap;

  private TextVar wCypher;

  private TableView wParameters;

  private TableView wReturns;

  private CypherMeta input;

  public CypherDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (CypherMeta) inputMetadata;

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

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( "Cypher" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
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

    Label wlCypherFromField = new Label( shell, SWT.RIGHT );
    wlCypherFromField.setText( "Get Cypher from input field? " );
    props.setLook( wlCypherFromField );
    FormData fdlCypherFromField = new FormData();
    fdlCypherFromField.left = new FormAttachment( 0, 0 );
    fdlCypherFromField.right = new FormAttachment( middle, -margin );
    fdlCypherFromField.top = new FormAttachment( lastControl, 2 * margin );
    wlCypherFromField.setLayoutData( fdlCypherFromField );
    wCypherFromField = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wCypherFromField );
    FormData fdCypherFromField = new FormData();
    fdCypherFromField.left = new FormAttachment( middle, 0 );
    fdCypherFromField.right = new FormAttachment( 100, 0 );
    fdCypherFromField.top = new FormAttachment( wlCypherFromField, 0, SWT.CENTER );
    wCypherFromField.setLayoutData( fdCypherFromField );
    lastControl = wCypherFromField;

    wCypherFromField.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        enableFields();
      }
    } );

    Label wlCypherField = new Label( shell, SWT.RIGHT );
    wlCypherField.setText( "Cypher input field" );
    props.setLook( wlCypherField );
    FormData fdlCypherField = new FormData();
    fdlCypherField.left = new FormAttachment( 0, 0 );
    fdlCypherField.right = new FormAttachment( middle, -margin );
    fdlCypherField.top = new FormAttachment( lastControl, 2 * margin );
    wlCypherField.setLayoutData( fdlCypherField );
    wCypherField = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCypherField );
    wCypherField.addModifyListener( lsMod );
    FormData fdCypherField = new FormData();
    fdCypherField.left = new FormAttachment( middle, 0 );
    fdCypherField.right = new FormAttachment( 100, 0 );
    fdCypherField.top = new FormAttachment( wlCypherField, 0, SWT.CENTER );
    wCypherField.setLayoutData( fdCypherField );
    lastControl = wCypherField;

    Label wlUnwind = new Label( shell, SWT.RIGHT );
    wlUnwind.setText( "Use UNWIND to collect parameter values? " );
    wlUnwind.setToolTipText( "Collect the specified parameters field data and expose it into a single variable to support UNWIND statements");
    props.setLook( wlUnwind );
    FormData fdlUnwind = new FormData();
    fdlUnwind.left = new FormAttachment( 0, 0 );
    fdlUnwind.right = new FormAttachment( middle, -margin );
    fdlUnwind.top = new FormAttachment( lastControl, 2 * margin );
    wlUnwind.setLayoutData( fdlUnwind );
    wUnwind = new Button( shell, SWT.CHECK | SWT.BORDER );
    props.setLook( wUnwind );
    FormData fdUnwind = new FormData();
    fdUnwind.left = new FormAttachment( middle, 0 );
    fdUnwind.right = new FormAttachment( 100, 0 );
    fdUnwind.top = new FormAttachment( wlUnwind, 0, SWT.CENTER );
    wUnwind.setLayoutData( fdUnwind );
    lastControl = wUnwind;

    wUnwind.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        enableFields();
      }
    } );

    wlUnwindMap = new Label( shell, SWT.RIGHT );
    wlUnwindMap.setText( "Name of UNWIND values map" );
    props.setLook( wlUnwindMap );
    FormData fdlUnwindMap = new FormData();
    fdlUnwindMap.left = new FormAttachment( 0, 0 );
    fdlUnwindMap.right = new FormAttachment( middle, -margin );
    fdlUnwindMap.top = new FormAttachment( lastControl, 2 * margin );
    wlUnwindMap.setLayoutData( fdlUnwindMap );
    wUnwindMap = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUnwindMap );
    wUnwindMap.addModifyListener( lsMod );
    FormData fdUnwindMap = new FormData();
    fdUnwindMap.left = new FormAttachment( middle, 0 );
    fdUnwindMap.right = new FormAttachment( 100, 0 );
    fdUnwindMap.top = new FormAttachment( wlUnwindMap, 0, SWT.CENTER );
    wUnwindMap.setLayoutData( fdUnwindMap );
    lastControl = wUnwindMap;

    Label wlCypher = new Label( shell, SWT.LEFT );
    wlCypher.setText( "Cypher:" );
    props.setLook( wlCypher );
    FormData fdlCypher = new FormData();
    fdlCypher.left = new FormAttachment( 0, 0 );
    fdlCypher.right = new FormAttachment( middle, -margin );
    fdlCypher.top = new FormAttachment( lastControl, margin );
    wlCypher.setLayoutData( fdlCypher );
    wCypher = new TextVar( transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    wCypher.getTextWidget().setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wCypher );
    wCypher.addModifyListener( lsMod );
    FormData fdCypher = new FormData();
    fdCypher.left = new FormAttachment( 0, 0 );
    fdCypher.right = new FormAttachment( 100, 0 );
    fdCypher.top = new FormAttachment( wlCypher, margin );
    fdCypher.bottom = new FormAttachment( wlCypher, 350 + margin );
    wCypher.setLayoutData( fdCypher );
    lastControl = wCypher;
    
    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, null );

    String[] fieldNames;
    try {
      fieldNames = transMeta.getPrevStepFields( stepname ).getFieldNames();
    } catch ( Exception e ) {
      logError( "Unable to get fields from previous steps", e );
      fieldNames = new String[] {};
    }

    // Table: parameter and field
    //
    ColumnInfo[] parameterColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Parameter", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
        new ColumnInfo( "Neo4j Type", ColumnInfo.COLUMN_TYPE_CCOMBO, GraphPropertyType.getNames(), false ),
      };

    Label wlParameters = new Label( shell, SWT.LEFT );
    wlParameters.setText( "Parameters" );
    props.setLook( wlParameters );
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment( 0, 0 );
    fdlParameters.right = new FormAttachment( middle, -margin );
    fdlParameters.top = new FormAttachment( lastControl, margin );
    wlParameters.setLayoutData( fdlParameters );

    Button wbGetParameters = new Button(shell, SWT.PUSH);
    wbGetParameters.setText( "Get parameters" );

    FormData fdbGetParameters = new FormData();
    fdbGetParameters.right = new FormAttachment( 100, 0 );
    fdbGetParameters.top = new FormAttachment( wlParameters, margin );
    wbGetParameters.setLayoutData( fdbGetParameters );
    wbGetParameters.addListener( SWT.Selection, (e) -> {
      try {
        RowMetaInterface r = transMeta.getPrevStepFields( stepMeta );

        BaseStepDialog.getFieldsFromPrevious( r, wParameters, 2, new int[] { 2 }, new int[] {}, -1, -1,
          ( item, valueMeta ) -> Neo4JOutputDialog.getPropertyNameTypePrimary( item, valueMeta, new int[] { 1 }, new int[] { 3 }, -1 )
        );
      } catch(Exception ex) {
        new ErrorDialog(shell, "Error", "Error getting step input fields", ex);
      }
    }  );

    wParameters =
      new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, parameterColumns, input.getParameterMappings().size(), lsMod, props );
    props.setLook( wParameters );
    wParameters.addModifyListener( lsMod );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( wbGetParameters, -margin );
    fdParameters.top = new FormAttachment( wlParameters, margin );
    fdParameters.bottom = new FormAttachment( wlParameters, 200 + margin );
    wParameters.setLayoutData( fdParameters );
    lastControl = wParameters;

    // Table: return field name and type TODO Support more than String
    //
    ColumnInfo[] returnColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Return type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
      };

    Label wlReturns = new Label( shell, SWT.LEFT );
    wlReturns.setText( "Returns" );
    props.setLook( wlReturns );
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment( 0, 0 );
    fdlReturns.right = new FormAttachment( middle, -margin );
    fdlReturns.top = new FormAttachment( lastControl, margin );
    wlReturns.setLayoutData( fdlReturns );
    wReturns = new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wReturns );
    wReturns.addModifyListener( lsMod );
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment( 0, 0 );
    fdReturns.right = new FormAttachment( 100, 0 );
    fdReturns.top = new FormAttachment( wlReturns, margin );
    fdReturns.bottom = new FormAttachment( wOK, -2 * margin );
    wReturns.setLayoutData( fdReturns );
    // lastControl = wReturns;


    // Add listeners
    //
    wCancel.addListener( SWT.Selection, e->cancel() );
    wOK.addListener( SWT.Selection, e->ok() );
    wPreview.addListener( SWT.Selection, e-> preview() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wConnection.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );
    wBatchSize.addSelectionListener( lsDef );
    wCypherFromField.addSelectionListener( lsDef );
    wCypherField.addSelectionListener( lsDef );
    wUnwind.addSelectionListener( lsDef );
    wUnwindMap.addSelectionListener( lsDef );

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
    boolean fromField = wCypherFromField.getSelection();

    wCypher.setEnabled( !fromField );
    wCypherField.setEnabled( fromField );

    boolean usingUnwind = wUnwind.getSelection();

    wlUnwindMap.setEnabled( usingUnwind );
    wUnwindMap.setEnabled( usingUnwind );
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText( Const.NVL( input.getConnectionName(), "" ) );

    wCypherFromField.setSelection( input.isCypherFromField() );
    wCypherField.setText( Const.NVL( input.getCypherField(), "" ) );
    try {
      wCypherField.setItems( transMeta.getPrevStepFields( stepname ).getFieldNames() );
    } catch ( KettleStepException e ) {
      log.logError( "Error getting fields from previous steps", e );
    }

    wUnwind.setSelection( input.isUsingUnwind() );
    wUnwindMap.setText( Const.NVL( input.getUnwindMapName(), "" ) );

    // List of connections...
    //
    try {
      List<String> elementNames = NeoConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Neo4j connections", e );
    }

    wBatchSize.setText( Const.NVL( input.getBatchSize(), "" ) );
    wCypher.setText( Const.NVL( input.getCypher(), "" ) );

    for ( int i = 0; i < input.getParameterMappings().size(); i++ ) {
      ParameterMapping mapping = input.getParameterMappings().get( i );
      TableItem item = wParameters.table.getItem( i );
      item.setText( 1, Const.NVL( mapping.getParameter(), "" ) );
      item.setText( 2, Const.NVL( mapping.getField(), "" ) );
      item.setText( 3, Const.NVL( mapping.getNeoType(), "" ) );
    }
    wParameters.removeEmptyRows();
    wParameters.setRowNums();
    wParameters.optWidth( true );

    for ( int i = 0; i < input.getReturnValues().size(); i++ ) {
      ReturnValue returnValue = input.getReturnValues().get( i );
      TableItem item = wReturns.table.getItem( i );
      item.setText( 1, Const.NVL( returnValue.getName(), "" ) );
      item.setText( 2, Const.NVL( returnValue.getType(), "" ) );
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth( true );

    enableFields();
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(CypherMeta meta) {
    meta.setConnectionName( wConnection.getText() );
    meta.setBatchSize( wBatchSize.getText() );
    meta.setCypher( wCypher.getText() );

    meta.setCypherFromField( wCypherFromField.getSelection() );
    meta.setCypherField( wCypherField.getText() );

    meta.setUsingUnwind( wUnwind.getSelection() );
    meta.setUnwindMapName( wUnwindMap.getText() );

    List<ParameterMapping> mappings = new ArrayList<>();
    for ( int i = 0; i < wParameters.nrNonEmpty(); i++ ) {
      TableItem item = wParameters.getNonEmpty( i );
      mappings.add( new ParameterMapping( item.getText( 1 ), item.getText( 2 ), item.getText( 3 ) ) );
    }
    meta.setParameterMappings( mappings );

    List<ReturnValue> returnValues = new ArrayList<>();
    for ( int i = 0; i < wReturns.nrNonEmpty(); i++ ) {
      TableItem item = wReturns.getNonEmpty( i );
      returnValues.add( new ReturnValue( item.getText( 1 ), item.getText( 2 ) ) );
    }
    meta.setReturnValues( returnValues );
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

  private synchronized void preview() {
    CypherMeta oneMeta = new CypherMeta();
    this.getInfo(oneMeta);
    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(this.transMeta, oneMeta, this.wStepname.getText());
    this.transMeta.getVariable("Internal.Transformation.Filename.Directory");
    previewMeta.getVariable("Internal.Transformation.Filename.Directory");
    EnterNumberDialog
      numberDialog = new EnterNumberDialog(this.shell, this.props.getDefaultPreviewSize(),
      BaseMessages.getString(PKG, "CypherDialog.PreviewSize.DialogTitle"),
      BaseMessages.getString(PKG, "CypherDialog.PreviewSize.DialogMessage")
    );
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(this.shell, previewMeta, new String[]{this.wStepname.getText()}, new int[]{previewSize});
      progressDialog.open();
      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();
      if (!progressDialog.isCancelled() && trans.getResult() != null && trans.getResult().getNrErrors() > 0L) {
        EnterTextDialog
          etd = new EnterTextDialog(this.shell, BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title", new String[0]), BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message", new String[0]), loggingText, true);
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd = new PreviewRowsDialog(this.shell, this.transMeta, 0, this.wStepname.getText(), progressDialog.getPreviewRowsMeta(this.wStepname.getText()), progressDialog.getPreviewRows(this.wStepname.getText()), loggingText);
      prd.open();
    }
  }
}
