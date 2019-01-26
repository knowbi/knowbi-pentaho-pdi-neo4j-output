package bi.know.kettle.neo4j.steps.importer;

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
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class ImporterDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = ImporterMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;

  private CCombo wFilenameField;
  private CCombo wFileTypeField;
  private TextVar wDatabaseFilename;
  private TextVar wAdminCommand;
  private TextVar wBaseFolder;
  private TextVar wMaxMemory;
  private Button wHighIo;
  private Button wIgnoreDuplicateNodes;
  private Button wIgnoreMissingNodes;
  private Button wIgnoreExtraColumns;
  private Button wMultiLine;
  private Button wSkipBadRelationships;
  private TextVar wReadBufferSize;

  private ImporterMeta input;

  public ImporterDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (ImporterMeta) inputMetadata;

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
    shell.setText( "Neo4j Importer" );

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
    
    String[] fieldnames = new String[] {};
    try {
      fieldnames = transMeta.getPrevStepFields(stepMeta).getFieldNames();
    } catch ( KettleStepException e ) {
      log.logError("error getting input field names: ", e);
    }
    
    // Filename field
    //
    Label wlFilenameField = new Label( wComposite, SWT.RIGHT );
    wlFilenameField.setText( "Filename field " );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, 0 );
    fdlFilenameField.right = new FormAttachment( middle, -margin );
    fdlFilenameField.top = new FormAttachment( lastControl, 2 * margin );
    wlFilenameField.setLayoutData( fdlFilenameField );
    wFilenameField = new CCombo( wComposite, SWT.CHECK | SWT.BORDER );
    wFilenameField.setItems( fieldnames );
    props.setLook( wFilenameField );
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, 0 );
    fdFilenameField.right = new FormAttachment( 100, 0 );
    fdFilenameField.top = new FormAttachment( wlFilenameField, 0, SWT.CENTER );
    wFilenameField.setLayoutData( fdFilenameField );
    lastControl = wFilenameField;
    
    // FileType field
    //
    Label wlFileTypeField = new Label( wComposite, SWT.RIGHT );
    wlFileTypeField.setText( "File type field " );
    props.setLook( wlFileTypeField );
    FormData fdlFileTypeField = new FormData();
    fdlFileTypeField.left = new FormAttachment( 0, 0 );
    fdlFileTypeField.right = new FormAttachment( middle, -margin );
    fdlFileTypeField.top = new FormAttachment( lastControl, 2 * margin );
    wlFileTypeField.setLayoutData( fdlFileTypeField );
    wFileTypeField = new CCombo( wComposite, SWT.CHECK | SWT.BORDER );
    wFileTypeField.setItems( fieldnames );
    props.setLook( wFileTypeField );
    FormData fdFileTypeField = new FormData();
    fdFileTypeField.left = new FormAttachment( middle, 0 );
    fdFileTypeField.right = new FormAttachment( 100, 0 );
    fdFileTypeField.top = new FormAttachment( wlFileTypeField, 0, SWT.CENTER );
    wFileTypeField.setLayoutData( fdFileTypeField );
    lastControl = wFileTypeField;

    // The database filename to gencsv to
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

    // The max memory used
    //
    Label wlMaxMemory = new Label( wComposite, SWT.RIGHT );
    wlMaxMemory.setText( "Max memory) " );
    props.setLook( wlMaxMemory );
    FormData fdlMaxMemory = new FormData();
    fdlMaxMemory.left = new FormAttachment( 0, 0 );
    fdlMaxMemory.right = new FormAttachment( middle, -margin );
    fdlMaxMemory.top = new FormAttachment( lastControl, 2 * margin );
    wlMaxMemory.setLayoutData( fdlMaxMemory );
    wMaxMemory = new TextVar(transMeta, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxMemory );
    wMaxMemory.addModifyListener( lsMod );
    FormData fdMaxMemory = new FormData();
    fdMaxMemory.left = new FormAttachment( middle, 0 );
    fdMaxMemory.right = new FormAttachment( 100, 0 );
    fdMaxMemory.top = new FormAttachment( wlMaxMemory, 0, SWT.CENTER );
    wMaxMemory.setLayoutData( fdMaxMemory );
    lastControl = wMaxMemory;

    // High IO?
    //
    Label wlHighIo = new Label( wComposite, SWT.RIGHT );
    wlHighIo.setText( "High IO? " );
    props.setLook( wlHighIo );
    FormData fdlHighIo = new FormData();
    fdlHighIo.left = new FormAttachment( 0, 0 );
    fdlHighIo.right = new FormAttachment( middle, -margin );
    fdlHighIo.top = new FormAttachment( lastControl, 2 * margin );
    wlHighIo.setLayoutData( fdlHighIo );
    wHighIo = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wHighIo );
    FormData fdHighIo = new FormData();
    fdHighIo.left = new FormAttachment( middle, 0 );
    fdHighIo.right = new FormAttachment( 100, 0 );
    fdHighIo.top = new FormAttachment( wlHighIo, 0, SWT.CENTER );
    wHighIo.setLayoutData( fdHighIo );
    lastControl = wHighIo;

    // Ignore duplicate nodes?
    //
    Label wlIgnoreDuplicateNodes = new Label( wComposite, SWT.RIGHT );
    wlIgnoreDuplicateNodes.setText( "Ignore duplicate nodes? " );
    props.setLook( wlIgnoreDuplicateNodes );
    FormData fdlIgnoreDuplicateNodes = new FormData();
    fdlIgnoreDuplicateNodes.left = new FormAttachment( 0, 0 );
    fdlIgnoreDuplicateNodes.right = new FormAttachment( middle, -margin );
    fdlIgnoreDuplicateNodes.top = new FormAttachment( lastControl, 2 * margin );
    wlIgnoreDuplicateNodes.setLayoutData( fdlIgnoreDuplicateNodes );
    wIgnoreDuplicateNodes = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wIgnoreDuplicateNodes );
    FormData fdIgnoreDuplicateNodes = new FormData();
    fdIgnoreDuplicateNodes.left = new FormAttachment( middle, 0 );
    fdIgnoreDuplicateNodes.right = new FormAttachment( 100, 0 );
    fdIgnoreDuplicateNodes.top = new FormAttachment( wlIgnoreDuplicateNodes, 0, SWT.CENTER );
    wIgnoreDuplicateNodes.setLayoutData( fdIgnoreDuplicateNodes );
    lastControl = wIgnoreDuplicateNodes;

    // Ignore missing nodes?
    //
    Label wlIgnoreMissingNodes = new Label( wComposite, SWT.RIGHT );
    wlIgnoreMissingNodes.setText( "Ignore missing nodes? " );
    props.setLook( wlIgnoreMissingNodes );
    FormData fdlIgnoreMissingNodes = new FormData();
    fdlIgnoreMissingNodes.left = new FormAttachment( 0, 0 );
    fdlIgnoreMissingNodes.right = new FormAttachment( middle, -margin );
    fdlIgnoreMissingNodes.top = new FormAttachment( lastControl, 2 * margin );
    wlIgnoreMissingNodes.setLayoutData( fdlIgnoreMissingNodes );
    wIgnoreMissingNodes = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wIgnoreMissingNodes );
    FormData fdIgnoreMissingNodes = new FormData();
    fdIgnoreMissingNodes.left = new FormAttachment( middle, 0 );
    fdIgnoreMissingNodes.right = new FormAttachment( 100, 0 );
    fdIgnoreMissingNodes.top = new FormAttachment( wlIgnoreMissingNodes, 0, SWT.CENTER );
    wIgnoreMissingNodes.setLayoutData( fdIgnoreMissingNodes );
    lastControl = wIgnoreMissingNodes;

    // Ignore extra columns?
    //
    Label wlIgnoreExtraColumns = new Label( wComposite, SWT.RIGHT );
    wlIgnoreExtraColumns.setText( "Ignore extra columns? " );
    props.setLook( wlIgnoreExtraColumns );
    FormData fdlIgnoreExtraColumns = new FormData();
    fdlIgnoreExtraColumns.left = new FormAttachment( 0, 0 );
    fdlIgnoreExtraColumns.right = new FormAttachment( middle, -margin );
    fdlIgnoreExtraColumns.top = new FormAttachment( lastControl, 2 * margin );
    wlIgnoreExtraColumns.setLayoutData( fdlIgnoreExtraColumns );
    wIgnoreExtraColumns = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wIgnoreExtraColumns );
    FormData fdIgnoreExtraColumns = new FormData();
    fdIgnoreExtraColumns.left = new FormAttachment( middle, 0 );
    fdIgnoreExtraColumns.right = new FormAttachment( 100, 0 );
    fdIgnoreExtraColumns.top = new FormAttachment( wlIgnoreExtraColumns, 0, SWT.CENTER );
    wIgnoreExtraColumns.setLayoutData( fdIgnoreExtraColumns );
    lastControl = wIgnoreExtraColumns;

    // Whether or not fields from input source can span multiple lines
    //
    Label wlMultiLine = new Label( wComposite, SWT.RIGHT );
    wlMultiLine.setText( "Fields can have multi-line data? " );
    props.setLook( wlMultiLine );
    FormData fdlMultiLine = new FormData();
    fdlMultiLine.left = new FormAttachment( 0, 0 );
    fdlMultiLine.right = new FormAttachment( middle, -margin );
    fdlMultiLine.top = new FormAttachment( lastControl, 2 * margin );
    wlMultiLine.setLayoutData( fdlMultiLine );
    wMultiLine = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wMultiLine );
    FormData fdMultiLine = new FormData();
    fdMultiLine.left = new FormAttachment( middle, 0 );
    fdMultiLine.right = new FormAttachment( 100, 0 );
    fdMultiLine.top = new FormAttachment( wlMultiLine, 0, SWT.CENTER );
    wMultiLine.setLayoutData( fdMultiLine );
    lastControl = wMultiLine;

    // Whether or not to skip importing relationships that refers to missing node ids
    //
    Label wlSkipBadRelationships = new Label( wComposite, SWT.RIGHT );
    wlSkipBadRelationships.setText( "Skip bad relationships? " );
    props.setLook( wlSkipBadRelationships );
    FormData fdlSkipBadRelationships = new FormData();
    fdlSkipBadRelationships.left = new FormAttachment( 0, 0 );
    fdlSkipBadRelationships.right = new FormAttachment( middle, -margin );
    fdlSkipBadRelationships.top = new FormAttachment( lastControl, 2 * margin );
    wlSkipBadRelationships.setLayoutData( fdlSkipBadRelationships );
    wSkipBadRelationships = new Button(wComposite, SWT.CHECK | SWT.LEFT );
    props.setLook( wSkipBadRelationships );
    FormData fdSkipBadRelationships = new FormData();
    fdSkipBadRelationships.left = new FormAttachment( middle, 0 );
    fdSkipBadRelationships.right = new FormAttachment( 100, 0 );
    fdSkipBadRelationships.top = new FormAttachment( wlSkipBadRelationships, 0, SWT.CENTER );
    wSkipBadRelationships.setLayoutData( fdSkipBadRelationships );
    lastControl = wSkipBadRelationships;

    // Size of each buffer for reading input data. It has to at least be large enough
    // to hold the biggest single value in the input data.
    //
    Label wlReadBufferSize = new Label( wComposite, SWT.RIGHT );
    wlReadBufferSize.setText( "Read buffer size) " );
    props.setLook( wlReadBufferSize );
    FormData fdlReadBufferSize = new FormData();
    fdlReadBufferSize.left = new FormAttachment( 0, 0 );
    fdlReadBufferSize.right = new FormAttachment( middle, -margin );
    fdlReadBufferSize.top = new FormAttachment( lastControl, 2 * margin );
    wlReadBufferSize.setLayoutData( fdlReadBufferSize );
    wReadBufferSize = new TextVar(transMeta, wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wReadBufferSize );
    wReadBufferSize.addModifyListener( lsMod );
    FormData fdReadBufferSize = new FormData();
    fdReadBufferSize.left = new FormAttachment( middle, 0 );
    fdReadBufferSize.right = new FormAttachment( 100, 0 );
    fdReadBufferSize.top = new FormAttachment( wlReadBufferSize, 0, SWT.CENTER );
    wReadBufferSize.setLayoutData( fdReadBufferSize );
    lastControl = wReadBufferSize;
    

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
    wFilenameField.addSelectionListener( lsDef );
    wFileTypeField.addSelectionListener( lsDef );
    wDatabaseFilename.addSelectionListener( lsDef );
    wAdminCommand.addSelectionListener( lsDef );
    wBaseFolder.addSelectionListener( lsDef );
    wMaxMemory.addSelectionListener( lsDef );
    wReadBufferSize.addSelectionListener( lsDef );

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
    wFilenameField.setText( Const.NVL( input.getFilenameField(), "" ) );
    wFileTypeField.setText( Const.NVL( input.getFileTypeField(), "" ) );
    wDatabaseFilename.setText( Const.NVL( input.getDatabaseFilename(), "" ) );
    wAdminCommand.setText( Const.NVL( input.getAdminCommand(), "" ) );
    wBaseFolder.setText( Const.NVL( input.getBaseFolder(), "" ) );
    wMaxMemory.setText( Const.NVL( input.getMaxMemory(), "" ) );
    wHighIo.setSelection( input.isHighIo() );
    wIgnoreDuplicateNodes.setSelection( input.isIgnoringDuplicateNodes() );
    wIgnoreMissingNodes.setSelection( input.isIgnoringMissingNodes() );
    wIgnoreExtraColumns.setSelection( input.isIgnoringExtraColumns() );
    wSkipBadRelationships.setSelection( input.isSkippingBadRelationships());
    wMultiLine.setSelection( input.isMultiLine());
    wReadBufferSize.setText( Const.NVL( input.getReadBufferSize(), "" ) );

  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo( ImporterMeta meta) {
    meta.setFilenameField( wFilenameField.getText() );
    meta.setFileTypeField( wFileTypeField.getText() );
    meta.setAdminCommand( wAdminCommand.getText() );
    meta.setDatabaseFilename( wDatabaseFilename.getText() );
    meta.setBaseFolder( wBaseFolder.getText() );
    meta.setMaxMemory( wMaxMemory.getText() );
    meta.setHighIo( wHighIo.getSelection() );
    meta.setIgnoringDuplicateNodes( wIgnoreDuplicateNodes.getSelection() );
    meta.setIgnoringMissingNodes( wIgnoreMissingNodes.getSelection() );
    meta.setIgnoringExtraColumns( wIgnoreExtraColumns.getSelection() );
    meta.setSkippingBadRelationships( wSkipBadRelationships.getSelection() );
    meta.setMultiLine( wMultiLine.getSelection() );
    meta.setReadBufferSize( wReadBufferSize.getText() );
  }

}
