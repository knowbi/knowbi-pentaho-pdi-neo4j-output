package bi.know.kettle.neo4j.steps.graph;

import bi.know.kettle.neo4j.model.GraphModel;
import bi.know.kettle.neo4j.model.GraphModelUtils;
import bi.know.kettle.neo4j.shared.NeoConnection;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
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
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GraphOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = GraphOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;

  private CCombo wConnection;

  private CCombo wModel;
  private TextVar wBatchSize;
  private Button wCreateIndexes;

  private TableView wFieldMappings;


  private GraphOutputMeta input;

  private GraphModel activeModel;

  public GraphOutputDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta)inputMetadata, transMeta, stepname );
    input = (GraphOutputMeta) inputMetadata;
  }

  @Override
  public String open() {
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
    shell.setText( "Neo4j GraphOutput" );

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
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER);
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    // The connection line of items : Label, Combo, NewButton, EditButton
    //
    Label wlConnection = new Label( shell, SWT.RIGHT );
    wlConnection.setText( "Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2*margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString(PKG, "System.Button.Edit") );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( shell, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString(PKG, "System.Button.New") );
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


    // The model line of items : Label, Combo, NewButton, EditButton
    //
    Label wlModel = new Label( shell, SWT.RIGHT );
    wlModel.setText( "Model" );
    props.setLook( wlModel );
    FormData fdlModel = new FormData();
    fdlModel.left = new FormAttachment( 0, 0 );
    fdlModel.right = new FormAttachment( middle, -margin );
    fdlModel.top = new FormAttachment( lastControl, 2*margin );
    wlModel.setLayoutData( fdlModel );

    Button wEditModel = new Button( shell, SWT.PUSH | SWT.BORDER );
    wEditModel.setText( BaseMessages.getString(PKG, "System.Button.Edit") );
    FormData fdEditModel = new FormData();
    fdEditModel.top = new FormAttachment( wlModel, 0, SWT.CENTER );
    fdEditModel.right = new FormAttachment( 100, 0 );
    wEditModel.setLayoutData( fdEditModel );

    Button wNewModel = new Button( shell, SWT.PUSH | SWT.BORDER );
    wNewModel.setText( BaseMessages.getString(PKG, "System.Button.New") );
    FormData fdNewModel = new FormData();
    fdNewModel.top = new FormAttachment( wlModel, 0, SWT.CENTER );
    fdNewModel.right = new FormAttachment( wEditModel, -margin );
    wNewModel.setLayoutData( fdNewModel );

    wModel = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModel );
    wModel.addModifyListener( lsMod );
    FormData fdModel = new FormData();
    fdModel.left = new FormAttachment( middle, 0 );
    fdModel.right = new FormAttachment( wNewModel, -margin );
    fdModel.top = new FormAttachment( wlModel, 0, SWT.CENTER );
    wModel.setLayoutData( fdModel );
    lastControl = wModel;


    Label wlBatchSize = new Label( shell, SWT.RIGHT );
    wlBatchSize.setText( "Batch size (rows)" );
    props.setLook( wlBatchSize );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( middle, -margin );
    fdlBatchSize.top = new FormAttachment( lastControl, 2*margin );
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
    wlCreateIndexes.setToolTipText( "Create index on first row using label field and primary key properties." );
    props.setLook( wlCreateIndexes );
    FormData fdlCreateIndexes = new FormData();
    fdlCreateIndexes.left = new FormAttachment( 0, 0 );
    fdlCreateIndexes.right = new FormAttachment( middle, -margin );
    fdlCreateIndexes.top = new FormAttachment( lastControl, 2*margin );
    wlCreateIndexes.setLayoutData( fdlCreateIndexes );
    wCreateIndexes = new Button( shell, SWT.CHECK | SWT.BORDER );
    wCreateIndexes.setToolTipText( "Create index on first row using label field and primary key properties." );
    props.setLook( wCreateIndexes );
    FormData fdCreateIndexes = new FormData();
    fdCreateIndexes.left = new FormAttachment( middle, 0 );
    fdCreateIndexes.right = new FormAttachment( 100, 0 );
    fdCreateIndexes.top = new FormAttachment( wlCreateIndexes, 0, SWT.CENTER );
    wCreateIndexes.setLayoutData( fdCreateIndexes );
    lastControl = wCreateIndexes;


    // Some buttons at the bottom...
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    String[] fieldNames;
    try {
      fieldNames = transMeta.getPrevStepFields( stepname ).getFieldNames();
    } catch(Exception e) {
      logError("Unable to get fields from previous steps", e);
      fieldNames = new String[] {};
    }

    // Table: field to model mapping
    //
    ColumnInfo[] parameterColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field", ColumnInfo.COLUMN_TYPE_CCOMBO, fieldNames, false ),
        new ColumnInfo( "Target type", ColumnInfo.COLUMN_TYPE_CCOMBO, ModelTargetType.getNames(), false ),
        new ColumnInfo( "Target", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false ),
        new ColumnInfo( "Property", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0], false ),
      };

    Label wlFieldMappings = new Label( shell, SWT.LEFT );
    wlFieldMappings.setText( "Mappings..." );
    props.setLook( wlFieldMappings );
    FormData fdlFieldMappings = new FormData();
    fdlFieldMappings.left = new FormAttachment( 0, 0 );
    fdlFieldMappings.right = new FormAttachment( middle, -margin );
    fdlFieldMappings.top = new FormAttachment( lastControl, margin );
    wlFieldMappings.setLayoutData( fdlFieldMappings );
    wFieldMappings = new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, parameterColumns, input.getFieldModelMappings().size(), lsMod, props );
    props.setLook( wFieldMappings );
    wFieldMappings.addModifyListener( lsMod );
    FormData fdFieldMappings = new FormData();
    fdFieldMappings.left = new FormAttachment( 0, 0 );
    fdFieldMappings.right = new FormAttachment( 100, 0 );
    fdFieldMappings.top = new FormAttachment( wlFieldMappings, margin);
    fdFieldMappings.bottom = new FormAttachment( wOK, -margin*2);
    wFieldMappings.setLayoutData( fdFieldMappings );
    lastControl = wFieldMappings;

    // Add listeners
    lsCancel = e -> cancel();
    lsOK = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wConnection.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );
    wBatchSize.addSelectionListener( lsDef );

    wNewConnection.addListener( SWT.Selection, e -> newConnection() );
    wEditConnection.addListener( SWT.Selection, e-> editConnection() );
    wNewModel.addListener( SWT.Selection, e-> newModel() );
    wEditModel.addListener( SWT.Selection, e-> editModel() );

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

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText(Const.NVL(input.getConnectionName(), "") );
    updateConnectionsCombo();

    wModel.setText(Const.NVL(input.getModel(), ""));
    updateModelsCombo();


    wBatchSize.setText(Const.NVL(input.getBatchSize(), "") );
    wCreateIndexes.setSelection( input.isCreatingIndexes() );

    for ( int i = 0; i<input.getFieldModelMappings().size(); i++) {
      FieldModelMapping mapping = input.getFieldModelMappings().get( i );
      TableItem item = wFieldMappings.table.getItem( i );
      int idx=1;
      item.setText( idx++, Const.NVL(mapping.getField(), ""));
      item.setText( idx++, ModelTargetType.getCode( mapping.getTargetType() ));
      item.setText( idx++, Const.NVL(mapping.getTargetName(), ""));
      item.setText( idx++, Const.NVL(mapping.getTargetProperty(), ""));
    }
    wFieldMappings.removeEmptyRows();
    wFieldMappings.setRowNums();
    wFieldMappings.optWidth( true );

  }

  private void updateModelsCombo() {
    // List of models...
    //
    try {
      MetaStoreFactory<GraphModel> modelFactory = GraphModelUtils.getModelFactory( metaStore );
      List<String> modelNames = modelFactory.getElementNames();
      Collections.sort(modelNames);
      wModel.setItems(modelNames.toArray( new String[ 0 ] ));

      // Load the active model...
      //
      if (StringUtils.isNotEmpty( wModel.getText() )) {
        activeModel = modelFactory.loadElement( wModel.getText() );

        // Set combo boxes in the mappings...
        //
        wFieldMappings.getColumns()[2].setComboValues( activeModel.getNodeNames() );
      } else {
        activeModel = null;
      }

    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Unable to list Neo4j Graph Models", e );
    }
  }

  private void updateConnectionsCombo() {
    // List of connections...
    //
    try {
      List<String> elementNames = NeoConnectionUtils.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort(elementNames);
      wConnection.setItems(elementNames.toArray( new String[ 0 ] ));
    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Unable to list Neo4j connections", e );
    }
  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    input.setConnectionName( wConnection.getText() );
    input.setBatchSize( wBatchSize.getText() );
    input.setCreatingIndexes( wCreateIndexes.getSelection() );
    input.setModel( wModel.getText() );

    List<FieldModelMapping> mappings = new ArrayList<>();
    for ( int i = 0; i< wFieldMappings.nrNonEmpty(); i++) {
      TableItem item = wFieldMappings.getNonEmpty( i );
      int idx=1;
      String sourceField = item.getText(idx++);
      ModelTargetType targetType = ModelTargetType.parseCode( item.getText(idx++) );
      String targetName = item.getText(idx++);
      String targetProperty = item.getText(idx++);

      mappings.add( new FieldModelMapping(sourceField, targetType, targetName, targetProperty) );
    }
    input.setFieldModelMappings( mappings );



    dispose();
  }

  protected void newConnection() {
    NeoConnection connection = NeoConnectionUtils.newConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ) );
    if (connection!=null) {
      wConnection.setText(connection.getName());
      updateModelsCombo();
    }
  }

  protected void editConnection() {
    NeoConnectionUtils.editConnection( shell, transMeta, NeoConnectionUtils.getConnectionFactory( metaStore ), wConnection.getText() );
  }

  protected void newModel() {

    GraphModel model = GraphModelUtils.newModel( shell, GraphModelUtils.getModelFactory( metaStore ), getInputRowMeta() );
    if (model!=null) {
      wModel.setText(model.getName());
      updateModelsCombo();
    }
  }

  private RowMetaInterface getInputRowMeta() {
    RowMetaInterface inputRowMeta = null;
    try {
      inputRowMeta = transMeta.getPrevStepFields( stepname );
    } catch ( KettleStepException e ) {
      LogChannel.GENERAL.logError("Unable to find step input field", e);
    }
    return inputRowMeta;
  }

  protected void editModel() {
    GraphModelUtils.editModel( shell, GraphModelUtils.getModelFactory( metaStore ), wModel.getText(), getInputRowMeta() );
    updateModelsCombo();
  }
}
