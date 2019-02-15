package bi.know.kettle.neo4j.steps.split;

import bi.know.kettle.neo4j.core.data.GraphData;
import bi.know.kettle.neo4j.core.data.GraphNodeData;
import bi.know.kettle.neo4j.core.data.GraphRelationshipData;
import bi.know.kettle.neo4j.core.value.ValueMetaGraph;
import bi.know.kettle.neo4j.steps.gencsv.StreamConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
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

public class SplitGraph extends BaseStep implements StepInterface {

  private SplitGraphMeta meta;
  private SplitGraphData data;

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
  public SplitGraph( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (SplitGraphMeta) smi;
    data = (SplitGraphData) sdi;

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, getTransMeta(), getRepository(), getMetaStore() );

      data.graphFieldIndex = getInputRowMeta().indexOfValue( meta.getGraphField() );
      if ( data.graphFieldIndex < 0 ) {
        throw new KettleException( "Unable to find graph field " + meta.getGraphField() + "' in the step input" );
      }
      ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta( data.graphFieldIndex );
      if (valueMeta.getType()!= ValueMetaGraph.TYPE_GRAPH ) {
        throw new KettleException( "Please specify a Graph field to split" );
      }

      data.typeField = null;
      if (StringUtils.isNotEmpty( meta.getTypeField() )) {
        data.typeField = environmentSubstitute( meta.getTypeField() );
      }
      data.idField = null;
      if (StringUtils.isNotEmpty( meta.getIdField() )) {
        data.idField = environmentSubstitute( meta.getIdField() );
      }
      data.propertySetField = null;
      if (StringUtils.isNotEmpty( meta.getPropertySetField() )) {
        data.propertySetField = environmentSubstitute( meta.getPropertySetField() );
      }
    }

    ValueMetaGraph valueMeta = (ValueMetaGraph) getInputRowMeta().getValueMeta( data.graphFieldIndex );
    Object valueData = row[data.graphFieldIndex];
    GraphData graphData = valueMeta.getGraphData( valueData );

    for ( GraphNodeData nodeData : graphData.getNodes() ) {
      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = getInputRowMeta().size();
      GraphData copy = graphData.createEmptyCopy();
      copy.getNodes().add( nodeData.clone() );

      outputRowData[data.graphFieldIndex] = copy;
      if (data.typeField!=null) {
        outputRowData[ index++ ] = "Node";
      }
      if (data.idField!=null) {
        outputRowData[ index++ ] = nodeData.getId();
      }
      if (data.propertySetField!=null) {
        outputRowData[ index++ ] = nodeData.getPropertySetId();
      }
      putRow( data.outputRowMeta, outputRowData );
    }

    for ( GraphRelationshipData relationshipData : graphData.getRelationships() ) {
      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = getInputRowMeta().size();
      GraphData copy = graphData.createEmptyCopy();
      copy.getRelationships().add( relationshipData.clone() );

      outputRowData[data.graphFieldIndex] = copy;
      if (data.typeField!=null) {
        outputRowData[ index++ ] = "Relationship";
      }
      if (data.idField!=null) {
        outputRowData[ index++ ] = relationshipData.getId();
      }
      if (data.propertySetField!=null) {
        outputRowData[ index++ ] = relationshipData.getPropertySetId();
      }
      putRow( data.outputRowMeta, outputRowData );
    }

    return true;
  }

}
