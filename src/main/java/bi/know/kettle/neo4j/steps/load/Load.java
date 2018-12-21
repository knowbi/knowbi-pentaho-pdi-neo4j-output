package bi.know.kettle.neo4j.steps.load;

import bi.know.kettle.neo4j.core.data.GraphData;
import bi.know.kettle.neo4j.core.data.GraphNodeData;
import bi.know.kettle.neo4j.core.data.GraphPropertyData;
import bi.know.kettle.neo4j.core.data.GraphPropertyDataType;
import bi.know.kettle.neo4j.core.data.GraphRelationshipData;
import bi.know.kettle.neo4j.core.value.ValueMetaGraph;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Load extends BaseStep implements StepInterface {

  private LoadMeta meta;
  private LoadData data;

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
  public Load( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
               Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (LoadMeta) smi;
    data = (LoadData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      if ( data.nodesProcessed > 0 ) {
        loadBufferIntoDb();
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.nodesProcessed = 0L;

      data.graphFieldIndex = getInputRowMeta().indexOfValue( meta.getGraphFieldName() );
      if ( data.graphFieldIndex < 0 ) {
        throw new KettleException( "Unable to find graph field " + meta.getGraphFieldName() + "' in the step input" );
      }
      if ( getInputRowMeta().getValueMeta( data.graphFieldIndex ).getType() != ValueMetaGraph.TYPE_GRAPH ) {
        throw new KettleException( "Field " + meta.getGraphFieldName() + "' needs to be of type Graph" );
      }

      if ( StringUtils.isEmpty( meta.getAdminCommand() ) ) {
        data.adminCommand = "neo4j-admin";
      } else {
        data.adminCommand = environmentSubstitute( meta.getAdminCommand() );
      }
      if ( meta.getUniquenessStrategy() != UniquenessStrategy.None ) {
        data.indexedGraphData = new IndexedGraphData( meta.getUniquenessStrategy(), meta.getUniquenessStrategy() );
      } else {
        data.indexedGraphData = null;
      }
      data.databaseFilename = environmentSubstitute( meta.getDatabaseFilename() );
      data.reportFile = environmentSubstitute( meta.getReportFile() );

      data.connection = null;

      data.baseFolder = environmentSubstitute( meta.getBaseFolder() );
      if ( !data.baseFolder.endsWith( File.separator ) ) {
        data.baseFolder += File.separator;
      }

      data.importFolder = data.baseFolder + "import/";

    }

    if ( meta.getUniquenessStrategy() != UniquenessStrategy.None ) {

      // Add the row to the internal buffer...
      //
      addRowToBuffer( getInputRowMeta(), row );

    } else {

      // Get the graph data
      //
      ValueMetaGraph valueMetaGraph = (ValueMetaGraph) getInputRowMeta().getValueMeta( data.graphFieldIndex );
      GraphData graphData = valueMetaGraph.getGraphData( row[ data.graphFieldIndex ] );

      if ( !graphData.getNodes().isEmpty() ) {

        if ( data.nodesProcessed == 0L ) {

          try {
            data.nodeFilename = calculateNodeFilename();
            data.nodeOutputStream = new BufferedOutputStream( new FileOutputStream( data.nodeFilename ) );
          } catch ( Exception e ) {
            throw new KettleException( "Unable to create nodes CSV file '" + data.nodeFilename + "'", e );
          }

          // Write the Nodes CSV file header...
          //
          List<GraphPropertyData> properties = graphData.getNodes().get( 0 ).getProperties();
          data.nodeProps = new ArrayList<IdType>();
          for ( int i = 0; i < properties.size(); i++ ) {
            data.nodeProps.add( new IdType( properties.get( i ).getId(), properties.get( i ).getType() ) );
          }
          data.nodePropertyIndexes = new HashMap<>();
          for ( int i = 0; i < properties.size(); i++ ) {
            data.nodePropertyIndexes.put( properties.get( i ).getId(), i );
          }
          try {
            writeNodeCsvHeader( data.nodeOutputStream, data.nodeProps );
          } catch ( Exception e ) {
            throw new KettleException( "Error writing Node CSV file header ", e );
          }
        }

        // Write Nodes to disk
        //
        try {
          writeNodeCsvRows( data.nodeOutputStream, graphData.getNodes(), data.nodeProps, data.nodePropertyIndexes );
        } catch ( Exception e ) {
          throw new KettleException( "Error writing Node CSV node properties row", e );
        }

        data.nodesProcessed++;
      }

      if ( !graphData.getRelationships().isEmpty() ) {
        if ( data.relsProcessed == 0L ) {

          try {
            data.relsFilename = calculateRelatiohshipsFilename();
            data.relsOutputStream = new BufferedOutputStream( new FileOutputStream( data.relsFilename ) );
          } catch ( Exception e ) {
            throw new KettleException( "Unable to create relationships CSV file '" + data.relsFilename + "'", e );
          }

          // Write the Relationships CSV file header...
          //

          // The properties...
          //
          List<GraphPropertyData> properties = graphData.getRelationships().get( 0 ).getProperties();

          data.relProps = new ArrayList<>();
          for ( int i = 0; i < properties.size(); i++ ) {
            data.relProps.add( new IdType( properties.get( i ).getId(), properties.get( i ).getType() ) );
          }

          // Index these properties
          //
          data.relPropertyIndexes = new HashMap<>();
          for ( int index = 0; index < data.relProps.size(); index++ ) {
            data.relPropertyIndexes.put( data.relProps.get( index ).getId(), index );
          }

          try {
            writeRelsCsvHeader( data.relsOutputStream, data.nodeProps, data.relPropertyIndexes );
          } catch ( Exception e ) {
            throw new KettleException( "Error writing Node CSV file header ", e );
          }
        }

        // Write relationships to disk
        //
        try {
          writeRelsCsvRows( data.relsOutputStream, graphData.getRelationships(), data.relProps, data.relPropertyIndexes );
        } catch ( Exception e ) {
          throw new KettleException( "Error writing Relationships CSV node properties rows", e );
        }

        data.relsProcessed++;
      }

      // if ( data.batchSize > 0 && data.nodesProcessed >= data.batchSize ) {
      //  loadBufferIntoDb();
      // }
    }

    // Pay it forward
    //
    putRow( getInputRowMeta(), row );

    return true;
  }

  protected void loadBufferIntoDb() throws KettleException {

    try {

      if ( meta.getUniquenessStrategy() != UniquenessStrategy.None ) {

        // Create a file for the nodes in the import folder called node-0.csv (0 = copy number)
        //
        String nodesFile = createNodesFile();

        // Create a file for the relationships in the import folder called rels-0,csv (0 = copy number)
        //
        String relsFile = createRelationshipsFile();

        runImport( nodesFile, relsFile );
      } else {
        try {
          data.nodeOutputStream.flush();
          data.nodeOutputStream.close();
        } catch ( Exception e ) {
          throw new KettleException( "Unable to flush/close nodes output file '" + data.nodeFilename + "'", e );
        }
        try {
          if ( data.relsOutputStream != null ) {
            data.relsOutputStream.flush();
            data.relsOutputStream.close();
          }
        } catch ( Exception e ) {
          throw new KettleException( "Unable to flush/close relationships output file '" + data.relsFilename + "'", e );
        }

        // simply run the import
        //
        runImport( data.nodeFilename, data.relsFilename );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unable to load data into Neo4j using neo4j-admin import", e );
    }

  }

  private String createNodesFile() throws KettleException {

    String filename = calculateNodeFilename();

    OutputStream os = null;

    // Write the header first...
    // Which fields ?
    //
    try {

      os = new BufferedOutputStream( new FileOutputStream( filename ) );

      // The nodes
      //
      List<GraphNodeData> nodes = data.indexedGraphData.getNodes();

      // The properties...
      //
      Set<IdType> set = data.indexedGraphData.getNodePropertiesSet();
      List<IdType> props = new ArrayList<>( set );

      // Index these properties
      //
      Map<String, Integer> propertyIndexes = new HashMap<>();
      for ( int index = 0; index < props.size(); index++ ) {
        propertyIndexes.put( props.get( index ).getId(), index );
      }

      writeNodeCsvHeader( os, props );

      log.logBasic( "Writing " + nodes.size() + " nodes to file '" + filename + "'" );

      writeNodeCsvRows( os, nodes, props, propertyIndexes );

    } catch ( Exception e ) {
      throw new KettleException( "Unable to write nodes to file '" + filename + "'", e );
    } finally {
      if ( os != null ) {
        try {
          os.flush();
          os.close();
        } catch ( Exception e ) {
          throw new KettleException( "Unable to flush/close nodes file '" + filename + "'", e );
        }
      }
    }

    return filename;
  }


  private String calculateNodeShortFilename() {
    return "import/nodes-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateNodeFilename() {
    return data.importFolder + "nodes-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateRelatiohshipsShortFilename() {
    return "import/rels-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateRelatiohshipsFilename() {
    return data.importFolder + "rels-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private void writeNodeCsvHeader( OutputStream os, List<IdType> props ) throws KettleException, IOException {
    // Write the values to the file...
    //

    StringBuffer header = new StringBuffer();

    // The id...
    //
    header.append( "id:ID" );

    for ( IdType prop : props ) {

      header.append( "," );

      header.append( prop.getId() );
      if ( prop.getType() == null ) {
        throw new KettleException( "This step doesn't support importing data type '" + prop.getType().name() + "' yet." );
      }
      header.append( ":" ).append( prop.getType().getImportType() );

      GraphPropertyDataType type = prop.getType();
    }
    header.append(",:LABEL");
    header.append( Const.CR );

    System.out.println("NODES HEADER: '"+header+"'");
    os.write( header.toString().getBytes( "UTF-8" ) );
  }


  private void writeNodeCsvRows( OutputStream os, List<GraphNodeData> nodes, List<IdType> props, Map<String, Integer> propertyIndexes ) throws IOException {
    // Now we serialize the data...
    //
    for ( GraphNodeData node : nodes ) {

      StringBuffer row = new StringBuffer();
      row.append( '"' ).append( GraphPropertyData.escapeString( node.getId() ) ).append( '"' );

      // Get the properties in the right order of list props...
      //
      GraphPropertyData[] sortedProperties = new GraphPropertyData[ props.size() ];
      for ( GraphPropertyData prop : node.getProperties() ) {
        int index = propertyIndexes.get( prop.getId() );
        sortedProperties[ index ] = prop;
      }

      // Now write the list of properties to the file starting with the ID.
      //
      for ( int i = 0; i < sortedProperties.length; i++ ) {
        GraphPropertyData prop = sortedProperties[ i ];
        row.append( "," );
        if ( prop != null ) {
          if ( prop.getType() == GraphPropertyDataType.String ) {
            row.append( '"' ).append( prop.toString() ).append( '"' );
          } else {
            row.append( prop.toString() );
          }
        }
      }

      // Now write the labels for this node
      //
      for (int i=0;i<node.getLabels().size();i++) {
        if (i==0) {
          row.append(",");
        } else {
          row.append(";");
        }
        String label = node.getLabels().get(i);
        row.append( label );
      }
      row.append( Const.CR );

      // Write it out
      //
      os.write( row.toString().getBytes( "UTF-8" ) );
    }
  }

  private String createRelationshipsFile() throws KettleException {

    if ( data.indexedGraphData.getRelationships().isEmpty() ) {
      return null;
    }

    String filename = calculateRelatiohshipsFilename();

    OutputStream os = null;

    // Write the header first...
    // Which fields ?
    //
    try {

      os = new BufferedOutputStream( new FileOutputStream( filename ) );

      // The properties...
      //
      Set<IdType> set = data.indexedGraphData.getRelPropertiesSet();
      List<IdType> props = new ArrayList<>( set );

      log.logBasic( "Adding " + props.size() + " properties to relationships" );

      // Index these properties
      //
      Map<String, Integer> propertyIndexes = new HashMap<>();
      for ( int index = 0; index < props.size(); index++ ) {
        propertyIndexes.put( props.get( index ).getId(), index );
      }

      writeRelsCsvHeader( os, props, propertyIndexes );

      writeRelsCsvRows( os, data.indexedGraphData.getRelationships(), props, propertyIndexes );

    } catch ( Exception e ) {
      throw new KettleException( "Unable to write relationships to file '" + filename + "'", e );
    } finally {
      if ( os != null ) {
        try {
          os.flush();
          os.close();
        } catch ( Exception e ) {
          throw new KettleException( "Unable to flush/close relationships file '" + filename + "'", e );
        }
      }
    }

    return filename;
  }

  private void writeRelsCsvRows( OutputStream os, List<GraphRelationshipData> relationships, List<IdType> props, Map<String, Integer> propertyIndexes ) throws IOException {

    // Now write the actual rows of data...
    //
    for ( GraphRelationshipData relationship : relationships ) {

      StringBuffer row = new StringBuffer();
      row.append( '"' ).append( GraphPropertyData.escapeString( relationship.getSourceNodeId() ) ).append( '"' );

      // Get the properties in the right order of list props...
      //
      GraphPropertyData[] sortedProperties = new GraphPropertyData[ props.size() ];
      for ( GraphPropertyData prop : relationship.getProperties() ) {
        int index = propertyIndexes.get( prop.getId() );
        sortedProperties[ index ] = prop;
      }

      // Now write the list of properties to the file starting with the ID.
      //
      for ( int i = 0; i < sortedProperties.length; i++ ) {
        GraphPropertyData prop = sortedProperties[ i ];
        row.append( "," );
        if ( prop != null ) {

          if ( prop.getType() == GraphPropertyDataType.String ) {
            row.append( '"' ).append( prop.toString() ).append( '"' );
          } else {
            row.append( prop.toString() );
          }
        }
      }

      row.append( "," ).append( '"' ).append( GraphPropertyData.escapeString( relationship.getTargetNodeId() ) ).append( '"' );

      // Now write the labels for this node
      //
      row.append( "," ).append( relationship.getLabel() );
      row.append( Const.CR );

      // Write it out
      //
      os.write( row.toString().getBytes( "UTF-8" ) );
    }

  }

  private void writeRelsCsvHeader( OutputStream os, List<IdType> props, Map<String, Integer> propertyIndexes ) throws KettleException, IOException {
    StringBuffer header = new StringBuffer();

    header.append( ":START_ID" );

    for ( IdType prop : props ) {

      header.append( "," );

      header.append( prop.getId() );
      if ( prop.getType() == null ) {
        throw new KettleException( "This step doesn't support importing data type '" + prop.getType().name() + "' yet." );
      }
      header.append( ":" ).append( prop.getType().getImportType() );

      GraphPropertyDataType type = prop.getType();
    }

    header.append( ",:END_ID,:TYPE" ).append( Const.CR );

    os.write( header.toString().getBytes( "UTF-8" ) );
  }


  private void runImport( String nodesFile, String relsFile ) throws KettleException {


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
    arguments.add( "--high-io=true" );
    arguments.add( "--nodes=" + calculateNodeShortFilename() );
    arguments.add( "--report-file=" + data.reportFile );
    if ( StringUtils.isNotEmpty( relsFile ) ) {
      arguments.add( "--relationships=" + calculateRelatiohshipsShortFilename() );
    }
    // arguments.add("--ignore-duplicate-nodes=true");

    log.logBasic( "Running command : " + arguments );


    ProcessBuilder pb = new ProcessBuilder( arguments );
    pb.directory( new File( "/var/lib/neo4j" ) );
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

  protected void addRowToBuffer( RowMetaInterface inputRowMeta, Object[] row ) throws KettleException {

    try {

      ValueMetaGraph valueMetaGraph = (ValueMetaGraph) inputRowMeta.getValueMeta( data.graphFieldIndex );
      GraphData graphData = valueMetaGraph.getGraphData( row[ data.graphFieldIndex ] );

      // Add all nodes and relationship to the indexed graph...
      //
      for ( GraphNodeData node : graphData.getNodes() ) {
        data.indexedGraphData.addAndIndexNode( node );
      }

      for ( GraphRelationshipData relationship : graphData.getRelationships() ) {
        data.indexedGraphData.addAndIndexRelationship( relationship );
      }

      data.nodesProcessed++;
    } catch ( Exception e ) {
      throw new KettleException( "Error adding row to load buffer", e );
    }
  }
}
