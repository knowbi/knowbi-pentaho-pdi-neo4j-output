package bi.know.kettle.neo4j.steps.dimensiongraph;

import bi.know.kettle.neo4j.core.MetaStoreUtil;
import bi.know.kettle.neo4j.model.GraphPropertyType;
import bi.know.kettle.neo4j.shared.DriverSingleton;
import bi.know.kettle.neo4j.shared.NeoConnectionUtils;
import bi.know.kettle.neo4j.steps.BaseNeoStep;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.*;

public class DimensionGraph extends BaseNeoStep implements StepInterface {
    private static Class<?> PKG = DimensionGraph.class;
    private DimensionGraphMeta meta;
    private DimensionGraphData data;

    public DimensionGraph(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis ) {
        super(s, stepDataInterface, c, t, dis);
    }

    public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        meta = (DimensionGraphMeta) smi;
        data = (DimensionGraphData) sdi;

        Object[] row = getRow(); //get row from input rowset
        if( row == null ) {
            setOutputDone();
            return false;
        }

        if( first ) {
            first = false;

            data.inputRowMeta = getInputRowMeta().clone();
            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

            // Get the fields that need conversion to normal storage...
            // Modify the storage type of the input data...
            data.lazyList = new ArrayList<Integer>();
            for ( int i = 0; i < data.inputRowMeta.size(); i++ ) {
                ValueMetaInterface valueMeta = data.inputRowMeta.getValueMeta( i );
                if ( valueMeta.isStorageBinaryString() ) {
                    data.lazyList.add( i );
                    valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
                }
            }

            // The start date value field (if applicable)
            data.startDateFieldIndex = -1;
            if ( data.startDateChoice == DimensionGraphMeta.StartDateAlternativeType.FIELD_VALUE) {
                data.startDateFieldIndex = data.inputRowMeta.indexOfValue( meta.getStartDateFieldName() );
                if ( data.startDateFieldIndex < 0 ) {
                    throw new KettleStepException( BaseMessages.getString(
                            PKG, "Neo4jDimensionGraph.Exception.StartDateValueColumnNotFound", meta.getStartDateFieldName() ) );
                }
            }

            // steam label value fields
            data.labelStreamIndexes = new int[meta.getDimensionNodeLabels().length];
            for ( int i = 0; i < meta.getDimensionNodeLabels().length; i++ ) {
                data.labelStreamIndexes[i] = data.inputRowMeta.indexOfValue( meta.getDimensionNodeLabels()[ i ] );
                if ( data.labelStreamIndexes[i] < 0 && StringUtils.isEmpty( meta.getDimensionNodeLabelValues()[ i ] ) ) {
                    throw new KettleStepException( BaseMessages.getString(
                            PKG, "Neo4jDimensionGraph.Exception.LabelFieldNotFound") );
                }
            }
            // steam key value fields
            data.keyStreamIndexes = new int[meta.getKeyFieldsStream().length];
            data.keyLookupTypes = new GraphPropertyType[ meta.getKeyPropsLookupType().length ];
            for ( int i = 0; i < meta.getKeyFieldsStream().length; i++ ) {
                data.keyStreamIndexes[ i ] = data.inputRowMeta.indexOfValue( meta.getKeyFieldsStream()[i] );
                if ( data.keyStreamIndexes[ i ] < 0 ) { // couldn't find field!
                    throw new KettleStepException( BaseMessages.getString(
                            PKG, "Neo4jDimensionGraph.Exception.KeyFieldNotFound", meta.getKeyFieldsStream()[i] ) );
                }
                data.keyLookupTypes[ i ] = GraphPropertyType.parseCode( meta.getKeyPropsLookupType()[i] );
            }

            // Return value fields
            data.fieldStreamIndexes = new int[meta.getStreamFieldNames().length];
            data.propLookupTypes = new GraphPropertyType[ meta.getNodePropTypes().length ];
            for ( int i = 0; meta.getStreamFieldNames() != null && i < meta.getStreamFieldNames().length; i++ ) {
                if ( !DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[ i ] ) ) {
                    data.fieldStreamIndexes[i] = data.outputRowMeta.indexOfValue( meta.getStreamFieldNames()[ i ] );
                    if ( data.fieldStreamIndexes[ i ] < 0 ) {
                        throw new KettleStepException( BaseMessages.getString(
                                PKG, "Neo4jDimensionGraph.Exception.ReturnFieldNotFound", meta.getStreamFieldNames()[ i ] ) );
                    }
                    data.propLookupTypes[ i ] = GraphPropertyType.parseCode( meta.getNodePropTypes()[ i ] );
                } else {
                    data.fieldStreamIndexes[ i ] = -1;
                    data.propLookupTypes[ i ] = null;
                }

            }

            if ( !Utils.isEmpty( meta.getStreamDateField() ) ) {
                data.dateStreamIndex = data.inputRowMeta.indexOfValue( meta.getStreamDateField() );
            } else {
                data.dateStreamIndex = -1;
            }
            // Initialize the start date value in case we don't have one in the input rows
            //
            data.valueDateNow = determineDimensionUpdatedDate( row );

            // Create a session
            //
            data.session = data.driver.session();


            //data.notFoundTk = new Long( meta.getDatabaseMeta().getNotFoundTK( isAutoIncrement() ) );
            // if (meta.getKeyRename()!=null && meta.getKeyRename().length()>0) data.notFoundTk.setName(meta.getKeyRename());

            if ( getCopy() == 0 ) {
//                checkDimZero();
            }

        }// end of first line

        // convert row to normal storage...
        //
        for ( int lazyFieldIndex : data.lazyList ) {
            ValueMetaInterface valueMeta = getInputRowMeta().getValueMeta( lazyFieldIndex );
            row[lazyFieldIndex] = valueMeta.convertToNormalStorageType( row[lazyFieldIndex] );
        }

        try {

            //construct the lookup statement
            buildDimLookupCypher( data.outputRowMeta, row );

            Object[] outputRow = lookupValues( data.inputRowMeta, row ); // add new values to the row in rowset[0].
            putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

            if ( checkFeedback( getLinesRead() ) ) {
                if ( log.isBasic() ) {
                    logBasic( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.LineNumber" ) + getLinesRead() );
                }
            }
        } catch ( KettleException e ) {
            logError( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.StepCanNotContinueForErrors", e.getMessage() ) );
            logError( Const.getStackTracker( e ) );
            setErrors( 1 );
            stopAll();
            setOutputDone(); // signal end to receiver(s)
            return false;
        }

        return true;
    }


    private void buildDimLookupCypher( RowMetaInterface rowMeta, Object[] row) throws KettleException {
        //construct the dimension node labels
        String[] nodeLabels = getDimensionNodeLabels(meta.getDimensionNodeLabels(), meta.getDimensionNodeLabelValues(), getInputRowMeta(), row, data.labelStreamIndexes );
        data.dimensionLabelsClause = getNodeLabelClause( nodeLabels);

        //construct the key lookup clause
        String keyLookupClause = "";
        for ( int i = 0; i < meta.getKeyPropsLookup().length; i++ ) {
            if ( i != 0 ) {
                keyLookupClause += ", ";
            }
            ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.keyStreamIndexes[ i ] );
            Object valueData = row[ data.keyStreamIndexes[ i ] ];
            GraphPropertyType propertyType = data.keyLookupTypes[ i ];
            Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
            keyLookupClause += meta.getKeyPropsLookup()[i] + ":" + neoValue;
        }

        //construct the date to compare
        Object neoDateLookup =GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateLookup" ), data.valueDateNow );

        //construct the return clause
        if( data.returnClause == null ) {
            data.returnClause = "id(n) AS " + meta.getIdRename() + ", n." + meta.getVersionProp() + " AS version";
            String propLookupClause = "";
            if ( !Utils.isEmpty( meta.getNodePropNames() ) ) {
                for ( int i = 0; i < meta.getNodePropNames().length; i++ ) {
                    // Don't retrieve the fields without input
                    if ( !Utils.isEmpty( meta.getNodePropNames()[i] )
                            && !DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[i] ) ) {
                        propLookupClause += ", n." + meta.getNodePropNames()[i] + " AS " + meta.getNodePropNames()[i];
//                        if ( !Utils.isEmpty( meta.getStreamFieldNames()[i] )
//                                && !meta.getNodePropNames()[i].equals( meta.getStreamFieldNames()[i] ) ) {
//                            propLookupClause += " AS `" + meta.getStreamFieldNames()[i] + "`";
//                        }
                    }
                }
            }
            data.returnClause += propLookupClause;
        }

        /*
         * MATCH (n:Label{key1:keys[1], key2:keys[2] ...})-[rel1:DIMENSION_UPDATE]->(previousnode)
         * OPTIONAL MATCH (nextnode:Label)-[rel2:DIMENSION_UPDATE]->(n)
         * WHERE (n.validDateFrom <= datefield) and (n.validDateTo > datefield)
         * RETURN id(n) as technicalKey, n.version as version, n.props[1] as props[1], ........
         *
         */
        /*
         * MATCH (n:Label{key1:keys[1], key2:keys[2] ...})
         * WHERE (n.validDateFrom <= datefield) and (n.validDateTo > datefield)
         * RETURN id(n) as technicalKey, n.version as version, n.props[1] as props[1], ........
         */

        data.dimLookupCypher = "MATCH (n:" + data.dimensionLabelsClause + "{" + keyLookupClause + "}) "
                + "WHERE (n." + meta.getStartDateProp() + "<= $dateLookup ) and (n." + meta.getEndDateProp() + "> $dateLookup ) "
                + "RETURN " + data.returnClause + " limit 1;"+ Const.CR;

        logDetailed( "----------------------------------------------\n" );
        logDetailed( "Finished preparing dimension lookup statement:\n [" + data.dimLookupCypher + "]" );
    }



    private synchronized Object[] lookupValues(RowMetaInterface rowMeta, Object[] row ) throws KettleException {
        Object[] outputRow = new Object[data.outputRowMeta.size()];
        Record returnRow = null;

        Long technicalKey;  // auto-increment usage. the node internal id will be used as technical key
        Long valueVersion;
        Date valueDate = null;
        Date valueDateFrom = null;
        Date valueDateTo = null;

        // Determine the lookup date ("now") if we have a field that carries said
        // date.
        // If not, the system date is taken.
        valueDate = determineDimensionUpdatedDate( row );

        // Nothing found in the cache?
        // Perform the lookup in the database...
        //
        if (returnRow == null) {
            Object neoDateLookup =GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateLookup" ), data.valueDateNow );

            StatementResult result = data.session.writeTransaction( tx -> tx.run( data.dimLookupCypher, Values.parameters( "dateLookup", neoDateLookup) ) );
//            processSummary( result );
            if( result.hasNext() ) {
                returnRow = result.next();
            }
            incrementLinesInput();
        }

        // This next block of code handles the dimension key LOOKUP ONLY.
        // We handle this case where "update = false" first for performance reasons
        //
        if ( !meta.isUpdate() ) {
            if ( returnRow == null ) {
//                returnRow = new Record();
//                returnRow[0] = data.notFoundTk;

            }
        } else {
            // This is the "update=true" case where we update the dimension table...
            // It is an "Insert - update" algorithm for slowly changing dimensions
            //
            // The dimension entry was not found, we need to add it!
            //
            if ( returnRow == null ) {
                logDetailed( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.NoDimensionEntryFound" )
                        + data.dimensionLabelsClause + ")" );

                // Date range: ]-oo,+oo[
                //

                if ( data.startDateChoice == DimensionGraphMeta.StartDateAlternativeType.SYSDATE ) {
                    // use the time the step execution begins as the date from.
                    // before, the current system time was used. this caused an exclusion of the row in the
                    // lookup portion of the step that uses this 'valueDate' and not the current time.
                    // the result was multiple inserts for what should have been 1 [PDI-4317]
                    valueDateFrom = valueDate;
                } else {
                    valueDateFrom = data.min_date;
                }
                valueDateTo = data.max_date;
                valueVersion = 1L; // Versions always start at 1.

                returnRow = dimInsertForNewEntry( data.inputRowMeta, row, valueVersion, valueDateFrom, valueDateTo);
                technicalKey = Long.valueOf( returnRow.get( meta.getIdRename(), -1) );
                incrementLinesOutput();

                logDetailed( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.AddedDimensionEntry" )
                        + technicalKey );
            } else {
                //
                // The entry was found: do we need to insert, update or both?
                //
                // What's the key? The first value of the return row
                technicalKey = Long.valueOf( returnRow.get( meta.getIdRename(), -1) );
                valueVersion = Long.valueOf( returnRow.get( meta.getVersionProp(), 1) );

                logDetailed( BaseMessages.getString( PKG, "*******************************************" ) );
                logDetailed( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.DimensionEntryFound" )
                            + technicalKey );
                logDetailed( BaseMessages.getString( PKG, "*******************************************" ) );


                // Date range: ]-oo,+oo[
                valueDateFrom = meta.getMinDate();
                valueDateTo = meta.getMaxDate();

                // The other values, we compare with
                int cmp;

                // If everything is the same: don't do anything
                // If one of the fields is different: insert or update
                // If all changed fields have update = Y, update
                // If one of the changed fields has update = N, insert

                boolean insert = false;
                boolean identical = true;
                boolean punch = false;

                // Column lookup array - initialize to all -1
                if ( data.columnLookupArray == null ) {
                    data.columnLookupArray = new int[meta.getStreamFieldNames().length];
                    for ( int i = 0; i < data.columnLookupArray.length; i++ ) {
                        data.columnLookupArray[i] = -1;
                    }
                }
                String streamFieldName = null;
                String nodePropName = null;
                for ( int i = 0; i < meta.getStreamFieldNames().length; i++ ) {

                    if ( data.fieldStreamIndexes[i] >= 0 ) {
                        // Only compare real fields, not last updated row, last version, etc
                        //
                        ValueMetaInterface v1 = data.outputRowMeta.getValueMeta( data.fieldStreamIndexes[i] );
                        Object valueData1 = row[data.fieldStreamIndexes[i]];
                        streamFieldName = meta.getStreamFieldNames()[i];
                        nodePropName = meta.getNodePropNames()[i];
                        // find the returnRowMeta based on the field in the fieldLookup list
                        GraphPropertyType v2 = null;
                        Object valueData2 = null;
                        List<String> returnRowKeys = returnRow.keys();
                        // Fix for PDI-8122
                        // See if it's already been computed.
                        if ( data.columnLookupArray[i] == -1 ) {
                            // It hasn't been found yet - search the list and make sure we're comparing
                            // the right column to the right column.
                            if( returnRow.containsKey( nodePropName ) ) {
                                data.columnLookupArray[i] = returnRow.index( nodePropName );
                                v2 = data.propLookupTypes[ i ];
                                valueData2 = returnRow.get( data.columnLookupArray[i] );
                            } else {
                                // Reset to null because otherwise, we'll get a false finding at the end.
                                // This could be optimized to use a temporary variable to avoid the repeated set if necessary
                                // but it will never be as slow as the database lookup anyway
                                v2 = null;
                            }
                        } else {
                            // We have a value in the columnLookupArray - use the value stored there.
                            v2 = data.propLookupTypes[ i ];
                            valueData2 = returnRow.get( data.columnLookupArray[i] );
                        }
                        if ( v2 == null ) {
                            // If we made it here, then maybe someone tweaked the XML in the transformation
                            // and we're matching a stream column to a column that doesn't really exist. Throw an exception.
                            throw new KettleStepException( BaseMessages.getString(
                                    PKG, "Neo4jDimensionGraph.Exception.ErrorDetectedInMatchingLookupFields", meta.getStreamFieldNames()[i] ) );
                        }

                        try {
                            cmp = compare( v1, valueData1, v2, valueData2 );
                        } catch ( ClassCastException e ) {
                            throw e;
                        } catch ( KettleValueException kve ) {
                            throw new KettleValueException( BaseMessages.getString(
                                    PKG, "Neo4jDimensionGraph.Exception.ErrorDetectedInComparingFields", meta.getStreamFieldNames()[i] ) );

                        }

                        // Not the same and update = 'N' --> insert
                        if ( cmp != 0 ) {
                            identical = false;
                        }

                        // Field flagged for insert: insert
                        logDetailed( BaseMessages.getString( PKG, "meta.propUpdateType=" + meta.getNodePropUpdateType()[i] ) );
                        if ( cmp != 0 && meta.getNodePropUpdateType()[i] == DimensionGraphMeta.PropUpdateType.INSERT ) {
                            insert = true;
                        }

                        // Field flagged for punchthrough
                        if ( cmp != 0 && meta.getNodePropUpdateType()[i] == DimensionGraphMeta.PropUpdateType.PUNCH_THROUGH) {
                            punch = true;
                        }

                        if ( isRowLevel() ) {
                            logRowlevel( BaseMessages.getString(PKG, "Neo4jDimensionGraph.Log.ComparingValues",
                                    "" + v1, "" + v2, String.valueOf( cmp ),
                                    String.valueOf( identical ), String.valueOf( insert ), String.valueOf( punch ) ) );
                        }
                    }

                    //if ( !identical ) {
                    //    break;
                    //}
                }

                // After comparing the record in the database and the data in the input
                // and taking into account the rules of the slowly changing dimension,
                // we found out whether or not to perform an insert or an update.
                //
                if ( !insert ) { // Just an update of row at key = valueKey
                    if ( !identical ) {
                        if ( isRowLevel() ) {
                            logRowlevel( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.UpdateRowWithValues" )
                                    + data.inputRowMeta.getString( row ) );
                        }
                        /*
                         * UPDATE d_customer SET fieldlookup[] = row.getValue(fieldnrs) WHERE returnkey = dimkey
                         */
                        dimUpdate( rowMeta, row, technicalKey, valueDate );
                        incrementLinesUpdated();

                    } else {
                        if ( isRowLevel() ) {
                            logRowlevel( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.SkipLine" ) );
                        }
                        // Don't do anything, everything is file in de dimension.
                        incrementLinesSkipped();
                    }
                } else {
                    if ( isRowLevel() ) {
                        logRowlevel( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.InsertNewVersion" )
                                + technicalKey.toString() );
                    }

                    Long valueNewVersion = valueVersion + 1;
                    // From date (valueDate) is calculated at the start of this method to
                    // be either the system date or the value in a column
                    //
                    valueDateFrom = valueDate;
                    valueDateTo = data.max_date;

                    // update our technicalKey with the return of the insert
                    returnRow = dimInsertForOldEntry( rowMeta, row, valueNewVersion, valueDateFrom, valueDateTo);
                    technicalKey = Long.valueOf( returnRow.get( meta.getIdRename(), -1) );
                    incrementLinesOutput();
                }
                if ( punch ) { // On of the fields we have to punch through has changed!
                    /*
                     * This means we have to update all versions:
                     *
                     * UPDATE dim SET punchf1 = val1, punchf2 = val2, ... WHERE fieldlookup[] = ? ;
                     *
                     * --> update ALL versions in the dimension table.
                     */
                    dimPunchThrough( rowMeta, row );
                    incrementLinesUpdated();
                }

//                returnRow = new Object[data.returnRowMeta.size()];
//                returnRow[0] = technicalKey;
                if ( isRowLevel() ) {
                    logRowlevel( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.TechnicalKey" ) + technicalKey );
                }
            }
        }

        if ( isRowLevel() ) {
            logRowlevel( BaseMessages.getString( PKG, "Neo4jDimensionGraph.Log.AddValuesToRow" ) + returnRow );
        }

        // Copy the results to the output row...
        //
        // First copy the input row values to the output..
        //
        for ( int i = 0; i < rowMeta.size(); i++ ) {
            outputRow[i] = row[i];
        }

        int outputIndex = rowMeta.size();
        int inputIndex = 0;

        // Then the technical key...
        //

        if ( Long.valueOf(returnRow.get(inputIndex).asLong()) instanceof Long) {
            if ( isDebug() ) {
//                log.logDebug( "returnRow[0] instanceof Long : " + (returnRow.get(inputIndex) instanceof Long) );
                log.logDebug( "the technical key is : " + returnRow.get(inputIndex).asLong() );
            }
            outputRow[outputIndex++] = returnRow.get(inputIndex++).asLong();
        } else {
            if( !meta.isUpdate() & returnRow == null) {
                outputRow[outputIndex++] = null;
                inputIndex++;
            } else {
                throw new KettleValueException( BaseMessages.getString(
                        PKG, "Neo4jDimensionGraph.Exception.ErrorDetectedOnRetrieveTechnicalKey", returnRow.get(0) ) );
            }
        }

        // skip the version in the return row
        inputIndex++;

        // Then get the "extra fields"...
        // don't return date from-to fields, they can be returned when explicitly
        // specified in lookup fields.
        /**
         * TODO:
         * 1. the return value should be cast according the return type array
         */
        while ( inputIndex < returnRow.size() && outputIndex < outputRow.length ) {
            outputRow[outputIndex] = returnRow.get(inputIndex);
            outputIndex++;
            inputIndex++;
        }

        // Finaly, check the date range!
        /*
         * TODO: WTF is this??? [May be it makes sense to keep the return date from-to fields within min/max range, but even
         * then the code below is wrong]. Value date; if (data.datefieldnr>=0) date = row.getValue(data.datefieldnr); else
         * date = new Value("date", new Date()); // system date
         *
         * if (data.min_date.compare(date)>0) data.min_date.setValue( date.getDate() ); if (data.max_date.compare(date)<0)
         * data.max_date.setValue( date.getDate() );
         */

        return outputRow;
    }

    private int compare( ValueMetaInterface v1, Object data1, GraphPropertyType v2, Object data2 ) throws KettleValueException {
        logDetailed("------------cmp----------------");
        logDetailed("v1Type: "+v1 + " v1Value: "+data1);
        logDetailed("v2Type: "+v2 + " v2Value: "+data2);
        logDetailed("----------end cmp--------------");

        Object valueData2;
        switch ( v2 ) {
            case String:
                valueData2 = ((Value)data2).asString();
                break;
            case Boolean:
                valueData2 = ((Value) data2).asBoolean();
                break;
            case Date:
                valueData2 = ((Value) data2).asLocalDate();
                break;
            case LocalDateTime:
                valueData2 = ((Value) data2).asLocalDate();
                break;
            default:
                valueData2 = data2;
        }
        Object convertData1 = v2.convertFromKettle(v1, data1);
        if( convertData1.equals(valueData2) ) {
            return 0;
        } else {
            return -1;
        }
    }

    /**
     * This inserts new record into dimension Optionally, if the entry already exists, update date range from previous
     * version of the entry.
     */
    public Record dimInsertForNewEntry( RowMetaInterface inputRowMeta, Object[] row, Long versionNr, Date dateFrom, Date dateTo) throws KettleException {

        Object neoVersionNr = GraphPropertyType.Integer.convertFromKettle(new ValueMetaInteger( "versionNr" ), versionNr );
        Object neoDateFrom = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateFrom" ), dateFrom );
        Object neoDateTo = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateTo" ), dateTo );

        //construct the dimension node labels
        String[] nodeLabels = getDimensionNodeLabels(meta.getDimensionNodeLabels(), meta.getDimensionNodeLabelValues(), getInputRowMeta(), row, data.labelStreamIndexes );
        data.dimensionLabelsClause = getNodeLabelClause( nodeLabels);

        //build the map for keys & props
        Map<String, Object> keysMap = new HashMap<>();
        Map<String, Object> propsMap = new HashMap<>();

        for ( int i = 0; i < meta.getKeyPropsLookup().length; i++ ) {
            ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.keyStreamIndexes[ i ] );
            Object valueData = row[ data.keyStreamIndexes[ i ] ];

            GraphPropertyType propertyType = data.keyLookupTypes[ i ];
            Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );

            String propName = "";
            if ( StringUtils.isNotEmpty( meta.getKeyPropsLookup()[ i ] ) ) {
                propName = meta.getKeyPropsLookup()[ i ];
            } else {
                propName = valueMeta.getName(); // Take the name from the input field.
            }
            keysMap.put( propName, neoValue );
            propsMap.put( propName, neoValue );
        }

        //build the map for props (should include those keys)
        for ( int i = 0; i < meta.getNodePropNames().length; i++ ) {
            if( !DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[ i ] ) ) {
                ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.fieldStreamIndexes[ i ] );
                Object valueData = row[ data.fieldStreamIndexes[ i ] ];
                GraphPropertyType propertyType = data.propLookupTypes[ i ];
                Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
                String propName = "";
                if ( StringUtils.isNotEmpty( meta.getNodePropNames()[ i ] ) ) {
                    propName = meta.getNodePropNames()[ i ];
                } else {
                    propName = meta.getStreamFieldNames()[ i ]; // Take the name from the input field.
                }
                propsMap.put( propName, neoValue );
            } else {
                Object neoValue = null;
                switch( meta.getNodePropUpdateType()[ i ] ) {
                    case DATE_INSERTED_UPDATED:
                    case DATE_INSERTED:
                        neoValue = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate(meta.getNodePropNames()[i]), new Date());
                        break;
                    case LAST_VERSION:
                        neoValue = GraphPropertyType.Boolean.convertFromKettle(new ValueMetaBoolean(meta.getNodePropNames()[i]), true);
                        break;
                    default:
                        break;
                }
                if( neoValue != null ) {
                    String propName = meta.getNodePropNames()[ i ];
                    propsMap.put( propName, neoValue );
                }
            }
        }

        switch ( data.startDateChoice ) {
            case NONE:
                propsMap.put( meta.getStartDateProp(), neoDateFrom );
                break;
            case SYSDATE:
                propsMap.put( meta.getStartDateProp(), neoDateFrom );
                break;
            case START_OF_TRANS:
                propsMap.put( meta.getStartDateProp(), getTrans().getStartDate() );
                break;
            case NULL:
                propsMap.put( meta.getStartDateProp(), null );
                break;
            case FIELD_VALUE:
                propsMap.put( meta.getStartDateProp(), row[ data.startDateFieldIndex ] );
                break;
            default:
                throw new KettleStepException( BaseMessages.getString(
                        PKG, "Neo4jDimensionGraph.Exception.IllegalStartDateSelection", data.startDateChoice.getDesc() ) );
        }
        propsMap.put( meta.getEndDateProp(), neoDateTo );
        propsMap.put( meta.getVersionProp(), neoVersionNr );

        /*
         * CREATE (n:Labels)
         * set n = Props (key[]/prop[]/startdate/enddate/version/lastinsert/lastupdate.....)
         * return id(n) as technicalKey, n.version as version;
         */
        data.dimInsertCypher = "CREATE (n:" + data.dimensionLabelsClause + ") "
                + "SET n = $props "
                + "RETURN id(n) AS " + meta.getIdRename() + ", n." + meta.getVersionProp() + " AS version "+ Const.CR;

        logDetailed( "Finished preparing dimension Insert statement for new entry:\n [" + data.dimInsertCypher + "]" );

        StatementResult result = data.session.writeTransaction( tx -> tx.run( data.dimInsertCypher,
                Values.parameters( "props", propsMap)) );
        if( result.hasNext() ) {
            return result.next();
        } else {
            throw new KettleException( "Unable to create the dimension root node:" + Const.CR + data.dimInsertCypher );
        }

    }


    public Record dimInsertForOldEntry( RowMetaInterface inputRowMeta, Object[] row, Long versionNr, Date dateFrom, Date dateTo) throws KettleException {
        Object neoNewVersionNr =GraphPropertyType.Integer.convertFromKettle(new ValueMetaInteger( "newVersionNr" ), versionNr );
        Object neoOldVersionNr =GraphPropertyType.Integer.convertFromKettle(new ValueMetaInteger( "oldVersionNr" ), versionNr-1 );
        Object neoDateFrom =GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateFrom" ), dateFrom );
        Object neoDateTo = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate( "dateTo" ), dateTo );

        //construct the dimension node labels
        String[] nodeLabels = getDimensionNodeLabels(meta.getDimensionNodeLabels(), meta.getDimensionNodeLabelValues(), getInputRowMeta(), row, data.labelStreamIndexes );
        data.dimensionLabelsClause = getNodeLabelClause( nodeLabels);
        //build the map for keys & props
        Map<String, Object> keysMap = new HashMap<>();
        Map<String, Object> propsMap = new HashMap<>();
        Map<String, Object> oldEntryPropsMap = new HashMap<>();
        String keyLookupClause = "";

        for ( int i = 0; i < meta.getKeyPropsLookup().length; i++ ) {
            ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.keyStreamIndexes[ i ] );
            Object valueData = row[ data.keyStreamIndexes[ i ] ];
            GraphPropertyType propertyType = data.keyLookupTypes[ i ];
            Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
            String propName = "";
            if ( StringUtils.isNotEmpty( meta.getKeyPropsLookup()[ i ] ) ) {
                propName = meta.getKeyPropsLookup()[ i ];
            } else {
                propName = valueMeta.getName(); // Take the name from the input field.
            }
            keyLookupClause += propName + ":" + neoValue;
            keysMap.put( propName, neoValue );
            propsMap.put( propName, neoValue );
        }
        //build the map for props (should include those keys)
        for ( int i = 0; i < meta.getNodePropNames().length; i++ ) {
            if( !DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[ i ] ) ) {
                ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.fieldStreamIndexes[ i ] );
                Object valueData = row[ data.fieldStreamIndexes[ i ] ];
                GraphPropertyType propertyType = data.propLookupTypes[ i ];
                Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
                String propName = "";
                if ( StringUtils.isNotEmpty( meta.getNodePropNames()[ i ] ) ) {
                    propName = meta.getNodePropNames()[ i ];
                } else {
                    propName = meta.getStreamFieldNames()[ i ]; // Take the name from the input field.
                }
                propsMap.put( propName, neoValue );
            } else {
                Object neoValue = null;
                switch( meta.getNodePropUpdateType()[ i ] ) {
                    case DATE_INSERTED_UPDATED:
                    case DATE_INSERTED:
                        neoValue = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate(meta.getNodePropNames()[i]), new Date());
                        break;
                    case LAST_VERSION:
                        neoValue = GraphPropertyType.Boolean.convertFromKettle(new ValueMetaBoolean(meta.getNodePropNames()[i]), true);
                        break;
                    default:
                        break;
                }
                if( neoValue != null ) {
                    String propName = meta.getNodePropNames()[ i ];
                    propsMap.put( propName, neoValue );
                }
            }
        }
        switch ( data.startDateChoice ) {
            case NONE:
                propsMap.put( meta.getStartDateProp(), neoDateFrom );
                oldEntryPropsMap.put( meta.getEndDateProp(), neoDateFrom );
                break;
            case SYSDATE:
                propsMap.put( meta.getStartDateProp(), neoDateFrom );
                oldEntryPropsMap.put( meta.getEndDateProp(), neoDateFrom );
                break;
            case START_OF_TRANS:
                propsMap.put( meta.getStartDateProp(), getTrans().getStartDate() );
                oldEntryPropsMap.put( meta.getEndDateProp(), getTrans().getStartDate() );
                break;
            case NULL:
                propsMap.put( meta.getStartDateProp(), null );
                oldEntryPropsMap.put( meta.getEndDateProp(), null );
                break;
            case FIELD_VALUE:
                propsMap.put( meta.getStartDateProp(), row[ data.startDateFieldIndex ] );
                oldEntryPropsMap.put( meta.getEndDateProp(), row[ data.startDateFieldIndex ] );
                break;
            default:
                throw new KettleStepException( BaseMessages.getString(
                        PKG, "Neo4jDimensionGraph.Exception.IllegalStartDateSelection", data.startDateChoice.getDesc() ) );
        }
        propsMap.put( meta.getEndDateProp(), neoDateTo );
        propsMap.put( meta.getVersionProp(), neoNewVersionNr );


        // finish the map for special props of old entry
        for ( int i = 0; i < meta.getNodePropUpdateType().length; i++ ) {
            if( DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[ i ] ) ) {
                Object neoValue = null;
                switch( meta.getNodePropUpdateType()[ i ] ) {
                    case DATE_INSERTED_UPDATED:
                    case DATE_UPDATED:
                        neoValue = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate(meta.getNodePropNames()[i]), new Date());
                        break;
                    case LAST_VERSION:
                        neoValue = GraphPropertyType.Boolean.convertFromKettle(new ValueMetaBoolean(meta.getNodePropNames()[i]), false);
                        break;
                    default:
                        break;
                }
                if( neoValue != null ) {
                    String propName = meta.getNodePropNames()[ i ];
                    oldEntryPropsMap.put( propName, neoValue );
                }
            }
        }

        /*
         * MATCH (n:Labels{key1:keys[1]...,version:oldVersionNr})
         * WITH n
         * SET n += oldEntryPropsMap
         * WITH n
         * CREATE (new:Labels)-[rel:DIMENSION_UPDATE]->(n)
         * SET new = propsMap (key[]/prop[]/startdate/enddate/version/lastinsert/lastupdate.....)
         * return id(n) as technicalKey, rel.version as version;
         */
        data.dimInsertCypher = "MATCH (old:" + data.dimensionLabelsClause + "{" + keyLookupClause + ", " + meta.getVersionProp() + ": $oldVersion }) "
                + "WITH old "
                + "SET old += $oldEntryPropsMap "
                + "WITH old "
                + "CREATE (new:" + data.dimensionLabelsClause + ")-[rel:" + meta.getRelLabelValue() + "]->(old) "
                + "SET new = $propsMap "
                + "RETURN id(new) AS " + meta.getIdRename() + ", rel." + meta.getVersionProp() + " AS version "+ Const.CR;

        logDetailed( "Finished preparing dimension Insert statement for old entry:\n [" + data.dimInsertCypher + "]" );

        StatementResult result = data.session.writeTransaction( tx -> tx.run( data.dimInsertCypher,
                Values.parameters("oldEntryPropsMap", oldEntryPropsMap, "propsMap", propsMap, "oldVersion", neoOldVersionNr) ) );
//        processSummary( result );

        if( result.hasNext() ) {
            return result.next();
        } else {
            throw new KettleException( "Unable to insert the dimension node:" + Const.CR + data.dimInsertCypher );
        }
    }

    public void dimUpdate( RowMetaInterface rowMeta, Object[] row, Long dimkey, Date valueDate ) throws KettleException {

        //build the map for props
        Map<String, Object> propsMap = new HashMap<>();

        for ( int i = 0; i < meta.getNodePropNames().length; i++ ) {
            if( !DimensionGraphMeta.isUpdateTypeWithoutArgument( meta.isUpdate(), meta.getNodePropUpdateType()[ i ] ) ) {
                ValueMetaInterface valueMeta = rowMeta.getValueMeta( data.fieldStreamIndexes[ i ] );
                Object valueData = row[ data.fieldStreamIndexes[ i ] ];
                GraphPropertyType propertyType = data.propLookupTypes[ i ];
                Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
                String propName = "";
                if ( StringUtils.isNotEmpty( meta.getNodePropNames()[ i ] ) ) {
                    propName = meta.getNodePropNames()[ i ];
                } else {
                    propName = valueMeta.getName(); // Take the name from the input field.
                }
                propsMap.put( propName, neoValue );
            } else {
                Object neoValueDate = null;
                switch( meta.getNodePropUpdateType()[ i ] ) {
                    case DATE_INSERTED_UPDATED:
                    case DATE_UPDATED:
                        ValueMetaInterface valueMeta = new ValueMetaDate( meta.getNodePropNames()[ i ] );
                        neoValueDate =GraphPropertyType.Date.convertFromKettle(valueMeta, valueDate );
                        break;
                        default:
                            break;
                }
                if( neoValueDate != null ) {
                    String propName = meta.getNodePropNames()[ i ];
                    propsMap.put( propName, neoValueDate );
                }
            }
        }

        Object nodeId =GraphPropertyType.Integer.convertFromKettle(new ValueMetaInteger( "dimKey" ), dimkey );

        /*
         * MERGE (n:Labels{id(n):dimKey})
         * ON MATCH SET n += Props
         */
        data.dimUpdateCypher = "MATCH (n) "
                + "WHERE id(n)=" + nodeId + " "
                + "SET n += $props " + Const.CR;

        logDetailed( "Finished preparing dimension Update statement: [" + data.dimUpdateCypher + "]" );

        StatementResult result = data.session.writeTransaction( tx -> tx.run( data.dimUpdateCypher,
                Values.parameters("props", propsMap) ) );
        processSummary( result );
    }

    public void dimPunchThrough( RowMetaInterface inputRowMeta, Object[] row) throws KettleException {
        Object neoDateNow = GraphPropertyType.Date.convertFromKettle(new ValueMetaDate("now"), new Date());
        Map<String, Object> propsMap = new HashMap<>();

        //construct the key lookup clause
        String keyLookupClause = "";
        for ( int i = 0; i < meta.getKeyPropsLookup().length; i++ ) {
            if ( i != 0 ) {
                keyLookupClause += ", ";
            }
            ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.keyStreamIndexes[ i ] );
            Object valueData = row[ data.keyStreamIndexes[ i ] ];
            GraphPropertyType propertyType = data.keyLookupTypes[ i ];
            Object neoValue = propertyType.convertFromKettle( valueMeta, valueData );
            keyLookupClause += meta.getKeyPropsLookup()[i] + ":" + neoValue;
        }
        for ( int i = 0; i < meta.getNodePropUpdateType().length; i++ ) {
            Object neoValue = null;
            switch( meta.getNodePropUpdateType()[ i ] ) {
                case PUNCH_THROUGH:
                    ValueMetaInterface valueMeta = inputRowMeta.getValueMeta( data.fieldStreamIndexes[ i ] );
                    Object valueData = row[ data.fieldStreamIndexes[ i ] ];
                    GraphPropertyType propertyType = data.propLookupTypes[ i ];
                    neoValue = propertyType.convertFromKettle( valueMeta, valueData );
                    String propName = "";
                    if ( StringUtils.isNotEmpty( meta.getNodePropNames()[ i ] ) ) {
                        propName = meta.getNodePropNames()[ i ];
                    } else {
                        propName = valueMeta.getName(); // Take the name from the input field.
                    }
                    propsMap.put( propName, neoValue);
                    break;
                case DATE_INSERTED_UPDATED:
                case DATE_UPDATED:
                    neoValue = neoDateNow;
                    break;
                default:
                    break;
            }
            if( neoValue != null ) {
                String propName = meta.getNodePropNames()[ i ];
                propsMap.put( propName, neoValue );
            }
        }
        /*
         * MATCH (n:Labels{key1:keys[1], key2:keys[2] ...})
         * WITH n
         * SET n += propsMap
         * return id(n) as technicalKey;
         */
        data.dimPunchCypher = "MATCH (n:" + data.dimensionLabelsClause + "{" + keyLookupClause + "}) "
                + "WITH n "
                + "SET n += $props "
                + "RETURN id(n) AS " + meta.getIdRename() + " "+ Const.CR;

        logDetailed( "Dimension Punch Through setting preparedStatement to [" + data.dimPunchCypher + "]" );
        logDetailed( "Finished preparing dimension punch statement." );

        StatementResult result = data.session.writeTransaction( tx -> tx.run( data.dimPunchCypher,
                Values.parameters("props", propsMap) ) );
        processSummary( result );
    }


    private void processSummary( StatementResult result ) throws KettleException {
        boolean error = false;
        ResultSummary summary = result.consume();
        for ( Notification notification : summary.notifications() ) {
            log.logError( notification.title() + " (" + notification.severity() + ")" );
            log.logError( notification.code() + " : " + notification.description() + ", position " + notification.position() );
            error = true;
        }
        if ( error ) {
            throw new KettleException( "Error found while executing cypher statement(s)" );
        }
    }

    private Date determineDimensionUpdatedDate(Object[] row ) throws KettleException {
        if ( data.dateStreamIndex < 0 ) {
            return getTrans().getCurrentDate(); // start of transformation...
        } else {
            Date rtn = data.inputRowMeta.getDate( row, data.dateStreamIndex ); // Date field in the input row
            if ( rtn != null ) {
                return rtn;
            } else {
                // Fix for PDI-4816
                String inputRowMetaStringMeta = null;
                try {
                    inputRowMetaStringMeta = data.inputRowMeta.toStringMeta();
                } catch ( Exception ex ) {
                    inputRowMetaStringMeta = "No row input meta";
                }
                throw new KettleStepException( BaseMessages.getString(
                        PKG, "Neo4jDimensionGraph.Exception.NullDimensionUpdatedDate", inputRowMetaStringMeta ) );
            }
        }
    }

    public String[] getDimensionNodeLabels( String[] labelFields, String[] labelValues, RowMetaInterface rowMeta, Object[] rowData, int[] labelIndexes ) throws KettleValueException {
        String[] nodeLabels = new String[ labelFields.length ];
        for ( int a = 0; a < labelFields.length; a++ ) {
            if ( StringUtils.isEmpty( labelFields[ a ] ) ) {
                nodeLabels[ a ] = environmentSubstitute( labelValues[ a ] );
            } else {
                nodeLabels[ a ] = rowMeta.getString( rowData, labelIndexes[ a ] );
            }
        }
        return nodeLabels;
    }

    private String getNodeLabelClause( String[] nodeLabels ) {
        String labels = "";
        for ( int i = 0; i < nodeLabels.length; i++ ) {
            if( i > 0 ) {
                labels += ":";
            }
            if ( nodeLabels[ i ].contains( " " ) || nodeLabels[ i ].contains( "." ) ) {
                nodeLabels[ i ] = "`" + nodeLabels[ i ] + "`";
            }
            labels += nodeLabels[ i ];
        }
        return labels;
    }


    private void wrapUpTransaction() throws KettleException {
    }

    public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
        meta = (DimensionGraphMeta) smi;
        data = (DimensionGraphData) sdi;

        if( super.init( smi, sdi ) ) {
            meta.normalizeAllocationFields();
            data.min_date = meta.getMinDate();
            data.max_date = meta.getMaxDate();
            data.startDateChoice = DimensionGraphMeta.StartDateAlternativeType.NONE;
            if( meta.isUsingStartDateAlternative() ) {
                data.startDateChoice = meta.getStartDateAlternativeType();
            }
            // Connect to Neo4j using info in Neo4j JDBC connection metadata...
            //
            if ( StringUtils.isEmpty( meta.getConnection() ) ) {
                log.logError( "You need to specify a Neo4j connection to use in this step" );
                return false;
            }
            try {
                // To correct lazy programmers who built certain PDI steps...
                //
                data.metaStore = MetaStoreUtil.findMetaStore( this );
                data.neoConnection = NeoConnectionUtils.getConnectionFactory( data.metaStore ).loadElement( meta.getConnection() );
                data.neoConnection.initializeVariablesFrom( this );
            } catch ( MetaStoreException e ) {
                log.logError( "Could not load Neo4j connection '" + meta.getConnection() + "' from the metastore", e );
                return false;
            }
            data.batchSize = Const.toLong( environmentSubstitute( meta.getBatchSize() ), 1 );
            try {
                data.driver = DriverSingleton.getDriver( log, data.neoConnection );
            } catch ( Exception e ) {
                log.logError( "Unable to get or create Neo4j database driver for database '" + data.neoConnection.getName() + "'", e );
                return false;
            }
            return true;
        }
        return false;
    }

    public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
        data = (DimensionGraphData) sdi;

        try {
            wrapUpTransaction();
        } catch ( KettleException e ) {
            logError( "Error wrapping up transaction", e );
            setErrors( 1L );
            stopAll();
        }

        if ( data.session != null ) {
            data.session.close();
        }

        super.dispose( smi, sdi );
    }
}
