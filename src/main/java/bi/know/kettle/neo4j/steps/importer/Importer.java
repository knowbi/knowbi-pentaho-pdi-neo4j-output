package bi.know.kettle.neo4j.steps.importer;

import bi.know.kettle.neo4j.steps.load.StreamConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Importer extends BaseStep implements StepInterface {

  private ImporterMeta meta;
  private ImporterData data;

  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public Importer( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                   Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (ImporterMeta) smi;
    data = (ImporterData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      if ( !data.nodesFiles.isEmpty() && !data.relsFiles.isEmpty() ) {
        runImport();
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.nodesFiles = new ArrayList<>();
      data.relsFiles = new ArrayList<>();

      data.filenameFieldIndex = getInputRowMeta().indexOfValue( meta.getFilenameField() );
      if ( data.filenameFieldIndex < 0 ) {
        throw new KettleException( "Unable to find filename field " + meta.getFilenameField() + "' in the step input" );
      }
      data.fileTypeFieldIndex = getInputRowMeta().indexOfValue( meta.getFileTypeField() );
      if ( data.fileTypeFieldIndex < 0 ) {
        throw new KettleException( "Unable to find file type field " + meta.getFileTypeField() + "' in the step input" );
      }

      if ( StringUtils.isEmpty( meta.getAdminCommand() ) ) {
        data.adminCommand = "neo4j-admin";
      } else {
        data.adminCommand = environmentSubstitute( meta.getAdminCommand() );
      }

      data.databaseFilename = environmentSubstitute( meta.getDatabaseFilename() );
      data.reportFile = environmentSubstitute( meta.getReportFile() );

      data.baseFolder = environmentSubstitute( meta.getBaseFolder() );
      if ( !data.baseFolder.endsWith( File.separator ) ) {
        data.baseFolder += File.separator;
      }

      data.importFolder = data.baseFolder + "import/";

    }

    String filename = getInputRowMeta().getString( row, data.filenameFieldIndex );
    String fileType = getInputRowMeta().getString( row, data.fileTypeFieldIndex );

    if ( StringUtils.isNotEmpty( filename ) && StringUtils.isNotEmpty( fileType ) ) {

      if ( "Node".equalsIgnoreCase( fileType ) ||
        "Nodes".equalsIgnoreCase( fileType ) ||
        "N".equalsIgnoreCase( fileType ) ) {
        data.nodesFiles.add( filename );
      }
      if ( "Relationship".equalsIgnoreCase( fileType ) ||
        "Relationships".equalsIgnoreCase( fileType ) ||
        "Rel".equalsIgnoreCase( fileType ) ||
        "Rels".equalsIgnoreCase( fileType ) ||
        "R".equalsIgnoreCase( fileType ) ||
        "Edge".equalsIgnoreCase( fileType ) ||
        "Edges".equalsIgnoreCase( fileType ) ||
        "E".equalsIgnoreCase( fileType ) ) {
        data.relsFiles.add( filename );
      }
    }

    // Pay it forward
    //
    putRow( getInputRowMeta(), row );

    return true;
  }


  private void runImport() throws KettleException {

    // See if we need to delete the existing database folder...
    //
    String targetDbFolder = data.baseFolder + "data/databases/" + data.databaseFilename;
    try {
      if ( new File( targetDbFolder ).exists() ) {
        log.logBasic( "Removing exsting folder: " + targetDbFolder );
        FileUtils.deleteDirectory( new File( targetDbFolder ) );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to remove old database files from '" + targetDbFolder + "'", e );
    }

    List<String> arguments = new ArrayList<>();

    arguments.add( data.adminCommand );
    arguments.add( "import" );
    arguments.add( "--database=" + data.databaseFilename + "" );
    arguments.add( "--id-type=STRING" );
    for ( String nodesFile : data.nodesFiles ) {
      arguments.add( "--nodes=" + nodesFile );
    }
    for ( String relsFile : data.relsFiles) {
      arguments.add( "--relationships=" + relsFile );
    }
    arguments.add( "--report-file=" + data.reportFile );
    arguments.add( "--high-io=" + ( meta.isHighIo() ? "true" : "false" ) );
    arguments.add( "--ignore-extra-columns=" + ( meta.isIgnoringExtraColumns() ? "true" : "false" ) );
    arguments.add( "--ignore-duplicate-nodes=" + ( meta.isIgnoringDuplicateNodes() ? "true" : "false" ) );
    arguments.add( "--ignore-missing-nodes=" + ( meta.isIgnoringMissingNodes() ? "true" : "false" ) );

    StringBuffer command = new StringBuffer();
    for ( String argument : arguments ) {
      command.append( argument ).append( " " );
    }
    log.logBasic( "Running command : " + command );
    log.logBasic( "Running from base folder: " + data.baseFolder );

    ProcessBuilder pb = new ProcessBuilder( arguments );
    pb.directory( new File( data.baseFolder ) );
    try {
      Process process = pb.start();

      StreamConsumer errorConsumer = new StreamConsumer( getLogChannel(), process.getErrorStream(), LogLevel.ERROR );
      errorConsumer.start();
      StreamConsumer outputConsumer = new StreamConsumer( getLogChannel(), process.getInputStream(), LogLevel.BASIC );
      outputConsumer.start();

      boolean exited = process.waitFor( 10, TimeUnit.MILLISECONDS );
      while ( !exited && !isStopped() ) {
        exited = process.waitFor( 10, TimeUnit.MILLISECONDS );
      }
      if ( !exited && isStopped() ) {
        process.destroyForcibly();
      }
    } catch ( Exception e ) {
      throw new KettleException( "Error running command: " + arguments, e );
    }
  }
}
