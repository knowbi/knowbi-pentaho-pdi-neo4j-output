package bi.know.kettle.neo4j.steps.gencsv;

import org.neo4j.kettle.core.data.GraphData;
import org.neo4j.kettle.core.data.GraphNodeData;
import org.neo4j.kettle.core.data.GraphPropertyData;
import org.neo4j.kettle.core.data.GraphPropertyDataType;
import org.neo4j.kettle.core.data.GraphRelationshipData;
import org.neo4j.kettle.core.value.ValueMetaGraph;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateCsv extends BaseStep implements StepInterface {

  private GenerateCsvMeta meta;
  private GenerateCsvData data;

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
  public GenerateCsv( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (GenerateCsvMeta) smi;
    data = (GenerateCsvData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      // Write data in the buffer to CSV files...
      //
      writeBufferToCsv();

      // Close all files and pass the filenames to the next steps.
      // These are stored in the fileMap
      //
      if ( data.fileMap != null ) {
        for ( CsvFile csvFile : data.fileMap.values() ) {

          try {
            csvFile.closeFile();
          } catch ( Exception e ) {
            setErrors( 1L );
            log.logError( "Error flushing/closing file '" + csvFile.getFilename() + "'", e );
          }

          Object[] nodeFileRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          nodeFileRow[ 0 ] = csvFile.getShortFilename();
          nodeFileRow[ 1 ] = csvFile.getFileType();
          putRow( data.outputRowMeta, nodeFileRow );
        }
      }

      if ( getErrors() > 0 ) {
        stopAll();
      }

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      data.fileMap = new HashMap<>();

      data.graphFieldIndex = getInputRowMeta().indexOfValue( meta.getGraphFieldName() );
      if ( data.graphFieldIndex < 0 ) {
        throw new KettleException( "Unable to find graph field " + meta.getGraphFieldName() + "' in the step input" );
      }
      if ( getInputRowMeta().getValueMeta( data.graphFieldIndex ).getType() != ValueMetaGraph.TYPE_GRAPH ) {
        throw new KettleException( "Field " + meta.getGraphFieldName() + "' needs to be of type Graph" );
      }

      if ( meta.getUniquenessStrategy() != UniquenessStrategy.None ) {
        data.indexedGraphData = new IndexedGraphData( meta.getUniquenessStrategy(), meta.getUniquenessStrategy() );
      } else {
        data.indexedGraphData = null;
      }
      data.filesPrefix = environmentSubstitute( meta.getFilesPrefix() );

      data.filenameField = environmentSubstitute( meta.getFilenameField() );
      data.fileTypeField = environmentSubstitute( meta.getFileTypeField() );

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

      // Now we're going to build a map between the input graph data property sets and the file the data will end up in.
      // This is stored in data.fileMap
      //
      if ( data.fileMap == null ) {
        data.fileMap = new HashMap<>();
      }

      // Process all the nodes and relationships in the graph data...
      // Write it all to CSV file
      //
      writeGraphDataToCsvFiles( graphData );
    }

    // Don't pass the rows to the next step, only the file names at the end.
    //
    return true;
  }

  protected void writeGraphDataToCsvFiles( GraphData graphData ) throws KettleException {

    if ( graphData == null ) {
      return;
    }

    // First update the CSV files...
    //
    writeNodesToCsvFiles( graphData );

    // Now do the relationships...
    //
    writeRelationshipsToCsvFiles( graphData );
  }

  protected void writeNodesToCsvFiles( GraphData graphData ) throws KettleException {
    for ( GraphNodeData nodeData : graphData.getNodes() ) {

      // The files we're generating for import need to have the same sets of properties
      // This set of properties is the same for every step generating the graph data types.
      // So we can use the name of the property step given by the step in combination with the
      // transformation and step name.
      //
      String propertySetKey = GenerateCsvData.getPropertySetKey(
        graphData.getSourceTransformationName(),
        graphData.getSourceStepName(),
        nodeData.getPropertySetId()
      );

      // See if we have this set already?
      //
      CsvFile csvFile = data.fileMap.get( propertySetKey );
      if ( csvFile == null ) {
        // Create a new file and write the header for the node...
        //
        String filename = calculateNodeFilename( propertySetKey );
        String shortFilename = calculateNodeShortFilename( propertySetKey );
        csvFile = new CsvFile( filename, shortFilename, "Nodes" );
        data.fileMap.put( propertySetKey, csvFile );

        try {
          csvFile.openFile();
        } catch ( Exception e ) {
          throw new KettleException( "Unable to create nodes CSV file '" + csvFile.getFilename() + "'", e );
        }

        // Calculate the unique list of node properties
        //
        List<GraphPropertyData> properties = nodeData.getProperties();
        for ( int i = 0; i < properties.size(); i++ ) {
          csvFile.getPropsList().add( new IdType( properties.get( i ).getId(), properties.get( i ).getType() ) );
        }
        for ( int i = 0; i < properties.size(); i++ ) {
          csvFile.getPropsIndexes().put( properties.get( i ).getId(), i );
        }
        csvFile.setIdFieldName( "id" );
        for ( GraphPropertyData property : properties ) {
          if ( property.isPrimary() ) {
            csvFile.setIdFieldName( property.getId() );
            break; // First ID found
          }
        }

        try {
          writeNodeCsvHeader( csvFile.getOutputStream(), csvFile.getPropsList(), csvFile.getIdFieldName() );
        } catch ( Exception e ) {
          throw new KettleException( "Unable to write node header to file '" + csvFile.getFilename() + "'", e );
        }
      }

      // Write a node data row to the CSV file...
      //
      try {
        writeNodeCsvRows( csvFile.getOutputStream(), Arrays.asList( nodeData ), csvFile.getPropsList(), csvFile.getPropsIndexes(), csvFile.getIdFieldName() );
      } catch ( Exception e ) {
        throw new KettleException( "Unable to write node header to file '" + csvFile.getFilename() + "'", e );
      }
    }
  }

  protected void writeRelationshipsToCsvFiles( GraphData graphData ) throws KettleException {
    for ( GraphRelationshipData relationshipData : graphData.getRelationships() ) {

      // The files we're generating for import need to have the same sets of properties
      // This set of properties is the same for every step generating the graph data types.
      // So we can use the name of the property step given by the step in combination with the
      // transformation and step name.
      //
      String propertySetKey = GenerateCsvData.getPropertySetKey(
        graphData.getSourceTransformationName(),
        graphData.getSourceStepName(),
        relationshipData.getPropertySetId()
      );

      // See if we have this set already?
      //
      CsvFile csvFile = data.fileMap.get( propertySetKey );
      if ( csvFile == null ) {
        // Create a new file and write the header for the node...
        //
        String filename = calculateRelatiohshipsFilename( propertySetKey );
        String shortFilename = calculateRelatiohshipsShortFilename( propertySetKey );
        csvFile = new CsvFile( filename, shortFilename, "Relationships" );
        data.fileMap.put( propertySetKey, csvFile );

        try {
          csvFile.openFile();
        } catch ( Exception e ) {
          throw new KettleException( "Unable to create relationships CSV file '" + csvFile.getFilename() + "'", e );
        }

        // Calculate the unique list of node properties
        //
        List<GraphPropertyData> properties = relationshipData.getProperties();
        for ( int i = 0; i < properties.size(); i++ ) {
          csvFile.getPropsList().add( new IdType( properties.get( i ).getId(), properties.get( i ).getType() ) );
        }
        for ( int i = 0; i < properties.size(); i++ ) {
          csvFile.getPropsIndexes().put( properties.get( i ).getId(), i );
        }
        csvFile.setIdFieldName( "id" );
        for ( GraphPropertyData property : properties ) {
          if ( property.isPrimary() ) {
            csvFile.setIdFieldName( property.getId() );
            break; // First ID found
          }
        }

        try {
          writeRelsCsvHeader( csvFile.getOutputStream(), csvFile.getPropsList(), csvFile.getPropsIndexes() );
        } catch ( Exception e ) {
          throw new KettleException( "Unable to write relationships header to file '" + csvFile.getFilename() + "'", e );
        }
      }

      // Write a relationships data row to the CSV file...
      //
      try {
        writeRelsCsvRows( csvFile.getOutputStream(), Arrays.asList( relationshipData ), csvFile.getPropsList(), csvFile.getPropsIndexes() );
      } catch ( Exception e ) {
        throw new KettleException( "Unable to write relationships header to file '" + csvFile.getFilename() + "'", e );
      }
    }
  }

  protected void writeBufferToCsv() throws KettleException {

    try {

      if ( meta.getUniquenessStrategy() != UniquenessStrategy.None ) {

        // Same logic
        //
        writeGraphDataToCsvFiles( data.indexedGraphData );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unable to generate CSV data for Neo4j import", e );
    }

  }

  private String calculateNodeShortFilename( String propertySetKey ) {
    return "import/" + Const.NVL( data.filesPrefix + "-", "" ) + "nodes-" + propertySetKey + "-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateNodeFilename( String propertySetKey ) {
    return data.importFolder + Const.NVL( data.filesPrefix + "-", "" ) + "nodes-" + propertySetKey + "-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateRelatiohshipsShortFilename( String propertySetKey ) {
    return "import/" + Const.NVL( data.filesPrefix + "-", "" ) + "rels-" + propertySetKey + "-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private String calculateRelatiohshipsFilename( String propertySetKey ) {
    return data.importFolder + Const.NVL( data.filesPrefix + "-", "" ) + "rels-" + propertySetKey + "-" + environmentSubstitute( "${" + Const.INTERNAL_VARIABLE_STEP_COPYNR + "}" ) + ".csv";
  }

  private void writeNodeCsvHeader( OutputStream os, List<IdType> props, String idFieldName ) throws KettleException, IOException {
    // Write the values to the file...
    //

    StringBuffer header = new StringBuffer();

    // The id...
    //
    header.append( idFieldName ).append( ":ID" );

    for ( IdType prop : props ) {

      if ( !prop.getId().equals( idFieldName ) ) {
        header.append( "," );

        header.append( prop.getId() );
        if ( prop.getType() == null ) {
          throw new KettleException( "This step doesn't support importing data type '" + prop.getType().name() + "' yet." );
        }
        header.append( ":" ).append( prop.getType().getImportType() );
      }
      // GraphPropertyDataType type = prop.getType();
    }
    header.append( ",:LABEL" );
    header.append( Const.CR );

    System.out.println( "NODES HEADER: '" + header + "'" );
    os.write( header.toString().getBytes( "UTF-8" ) );
  }


  private void writeNodeCsvRows( OutputStream os, List<GraphNodeData> nodes, List<IdType> props, Map<String, Integer> propertyIndexes, String idFieldName ) throws IOException {
    // Now we serialize the data...
    //
    for ( GraphNodeData node : nodes ) {

      StringBuffer row = new StringBuffer();

      // Get the properties in the right order of list props...
      //
      GraphPropertyData[] sortedProperties = new GraphPropertyData[ props.size() ];
      for ( GraphPropertyData prop : node.getProperties() ) {
        int index = propertyIndexes.get( prop.getId() );
        sortedProperties[ index ] = prop;
      }

      // First write the index field...
      //
      boolean indexFound = false;
      for ( int i = 0; i < sortedProperties.length; i++ ) {
        GraphPropertyData prop = sortedProperties[ i ];
        if ( prop.isPrimary() ) {
          if ( prop.getType() == GraphPropertyDataType.String ) {
            row.append( '"' ).append( prop.toString() ).append( '"' );
          } else {
            row.append( prop.toString() );
          }
          indexFound = true;
          break;
        }
      }
      // No index field? Take the node index
      //
      if ( !indexFound ) {
        row.append( '"' ).append( GraphPropertyData.escapeString( node.getId() ) ).append( '"' );
      }

      // Now write the other properties to the file
      //
      for ( int i = 0; i < sortedProperties.length; i++ ) {
        GraphPropertyData prop = sortedProperties[ i ];
        if ( !prop.isPrimary() ) {
          row.append( "," );
          if ( prop != null ) {
            if ( prop.getType() == GraphPropertyDataType.String ) {
              row.append( '"' ).append( prop.toString() ).append( '"' );
            } else {
              row.append( prop.toString() );
            }
          }
        }
      }

      // Now write the labels for this node
      //
      for ( int i = 0; i < node.getLabels().size(); i++ ) {
        if ( i == 0 ) {
          row.append( "," );
        } else {
          row.append( ";" );
        }
        String label = node.getLabels().get( i );
        row.append( label );
      }
      row.append( Const.CR );

      // Write it out
      //
      os.write( row.toString().getBytes( "UTF-8" ) );
    }
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

  protected void addRowToBuffer( RowMetaInterface inputRowMeta, Object[] row ) throws KettleException {

    try {

      ValueMetaGraph valueMetaGraph = (ValueMetaGraph) inputRowMeta.getValueMeta( data.graphFieldIndex );
      GraphData graphData = valueMetaGraph.getGraphData( row[ data.graphFieldIndex ] );

      // Add all nodes and relationship to the indexed graph...
      //
      for ( GraphNodeData node : graphData.getNodes() ) {

        // Copy the data and calculate a unique property set ID
        //
        GraphNodeData nodeCopy = new GraphNodeData( node );
        nodeCopy.setPropertySetId( graphData.getSourceTransformationName() + "-" + graphData.getSourceStepName() + "-" + node.getPropertySetId() );

        data.indexedGraphData.addAndIndexNode( nodeCopy );
      }

      for ( GraphRelationshipData relationship : graphData.getRelationships() ) {

        // Copy the data and calculate a unique property set ID
        //
        GraphRelationshipData relationshipCopy = new GraphRelationshipData( relationship );
        relationshipCopy.setPropertySetId( graphData.getSourceTransformationName() + "-" + graphData.getSourceStepName() + "-" + relationship.getPropertySetId() );

        data.indexedGraphData.addAndIndexRelationship( relationshipCopy );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Error adding row to gencsv buffer", e );
    }
  }
}
