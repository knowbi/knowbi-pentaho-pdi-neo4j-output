package bi.know.kettle.neo4j.steps.dimensiongraph;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * TODO:
 * 1. currently, not support caching
 * 2. currently, not support batch operation
 * 3. can be extended to support type-3 slowly changing dimensions update
 * 4. submit the sample project on github
 */
@Step(
        id = "Neo4jDimensionGraph",
        name = "Neo4jDimensionGraphMeta.Step.Name",
        image = "neo4j_dimensiongraph.svg",
        description = "Neo4jDimensionGraphMeta.Step.Description",
        categoryDescription = "Neo4jDimensionGraphMeta.Step.Category",
        documentationUrl = "https://github.com/knowbi/knowbi-pentaho-pdi-neo4j-output/wiki/Neo4j-DimensionGraph#description",
        i18nPackageName = "bi.know.kettle.neo4j.steps.dimensiongraph"
)
@InjectionSupported(
        localizationPrefix = "Neo4jDimensionGraph.Injection.",
        groups = {"NODE_LABELS", "NODE_KEYS", "NODE_PROPS", "REL_LABELS", "REL_PROPS"}
)
public class DimensionGraphMeta extends BaseStepMeta implements StepMetaInterface {
    private static Class<?> PKG = DimensionGraphMeta.class;

    private static final String STRING_CONNECTION = "connection";
    private static final String STRING_BATCH_SIZE = "batch_size";
    private static final String STRING_UPDATE = "update";
    private static final String STRING_LABELS = "labels";
    private static final String STRING_LABEL = "label";
    private static final String STRING_LABEL_VALUE = "label_value";
    private static final String STRING_KEYS = "keys";
    private static final String STRING_FIELD = "field";
    private static final String STRING_FIELD_RETURE_TYPE = "return_type";
    private static final String STRING_LOOKUP = "lookup";
    private static final String STRING_LOOKUP_TYPE = "lookup_type";
    private static final String STRING_PROPERTIES = "properties";
    private static final String STRING_PROPERTY = "property";
    private static final String STRING_PROPERTY_NAME = "property_name";
    private static final String STRING_PROPERTY_VALUE = "value";
    private static final String STRING_PROPERTY_TYPE = "type";
    private static final String STRING_PROPERTY_PRIMARY = "primary";
    private static final String STRING_PROPERTY_UPDATE_TYPE = "update_type";
    private static final String STRING_RELATIONSHIP = "relationship";
    private static final String STRING_RELATIONSHIP_VALUE = "relationship_value";
    private static final String STRING_RELPROPS = "relprops";
    private static final String STRING_REL_LABEL = "rel_label";
    private static final String STRING_START_DATE_PROP = "start_data_prop";
    private static final String STRING_END_DATE_PROP = "end_data_prop";
    private static final String STRING_VERSION_PROP = "version_prop";
    private static final String STRING_ID_RENAME = "id_rename";
    private static final String STRING_STREAM_DATE_FIELD = "data_field";
    private static final String STRING_MIN_YEAR = "min_year";
    private static final String STRING_MAX_YEAR = "max_year";
    private static final String STRING_USE_START_DATE_ALTERNATIVE = "use_start_date_alternative";
    private static final String STRING_START_DATE_ALTERNATIVE_TYPE = "start_date_alternative_type";
    private static final String STRING_START_DATE_FIELD_NAME = "start_date_prop_name";
    private static final String STRING_CREATE_INDEXES = "create_indexes";
    private static final String STRING_USE_CREATE = "use_create";
    private static final String STRING_ONLY_CREATE_RELATIONSHIPS = "only_create_relationships";


    enum StartDateAlternativeType {
        NONE("Neo4jDimensionGraphMeta.StartDateAlternative.None.Label"),
        SYSDATE("Neo4jDimensionGraphMeta.StartDateAlternative.Sysdate.Label"),
        START_OF_TRANS("Neo4jDimensionGraphMeta.StartDateAlternative.TransStart.Label"),
        NULL("Neo4jDimensionGraphMeta.StartDateAlternative.Null.Label"),
        FIELD_VALUE("Neo4jDimensionGraphMeta.StartDateAlternative.FieldValue.Label");
        private String desc;
        private StartDateAlternativeType(String desc) { this.desc = desc; }
        public String getDesc() { return BaseMessages.getString( PKG, desc); }
        public void setDesc(String desc) { this.desc = desc; }
    }

    /**
     * TODO:
     * 1. for add_property, the name of new property has to be indicated
     */
    enum PropUpdateType {
        INSERT("Neo4jDimensionGraphMeta.PropUpdateType.Insert.Label"),
        UPDATE("Neo4jDimensionGraphMeta.PropUpdateType.Update.Label"),
        //ADD_PROPERTY("Neo4jDimensionGraphMeta.PropUpdateType.AddProperty.Label"),
        PUNCH_THROUGH("Neo4jDimensionGraphMeta.PropUpdateType.PunchThrough.Label"),
        DATE_INSERTED_UPDATED("Neo4jDimensionGraphMeta.PropUpdateType.DateInsertedOrUpdated.Label"),
        DATE_INSERTED("Neo4jDimensionGraphMeta.PropUpdateType.DateInserted.Label"),
        DATE_UPDATED("Neo4jDimensionGraphMeta.PropUpdateType.DateUpdated.Label"),
        LAST_VERSION("Neo4jDimensionGraphMeta.PropUpdateType.LastVersion.Label");
        private String desc;
        private PropUpdateType(String desc) { this.desc = desc; }
        public String getDesc() { return BaseMessages.getString( PKG, desc); }
        public void setDesc(String desc) { this.desc = desc; }
        public static String[] getNames() {
            String[] result = new String[PropUpdateType.values().length];
            for( int i=0; i<PropUpdateType.values().length; i++) {
                result[ i ] = PropUpdateType.values()[ i ].name();
            }
            return result;
        }
    }

    public static final String[] typeDescLookup = ValueMetaFactory.getValueMetaNames();

    @Injection(name = "CONNECTION")
    private String connection;
    @Injection( name = "BATCH_SIZE" )
    private String batchSize;
    /** Update the dimension or just lookup? */
    @Injection( name = "UPDATE_DIMENSION" )
    private boolean update;
    /** Labels used to look up a node in the graph database */
    @Injection( name = "DIMENSION_NODE_LABEL", group = "NODE_LABELS" )
    private String[] dimensionNodeLabels;
    @Injection( name = "DIMENSION_NODE_LABEL_VALUE", group = "NODE_LABELS" )
    private String[] dimensionNodeLabelValues;
    /** Stream fields used to look up a value in the dimension node */
    @Injection( name = "KEY_STREAM_FIELD_NAME", group = "NODE_KEYS" )
    private String[] keyFieldsStream;
    /** Key properties of the dimension node to use for lookup */
    @Injection( name = "KEY_NODE_PROPERTY_NAME", group = "NODE_KEYS" )
    private String[] keyPropsLookup;
    /** Key properties of the dimension node to use for lookup */
    @Injection( name = "KEY_NODE_PROPERTY_TYPE", group = "NODE_KEYS" )
    private String[] keyPropsLookupType;
    /** Fields containing the values in the input stream to update the dimension node with */
    @Injection( name = "STREAM_FIELD_NAME", group = "NODE_PROPS" )
    private String[] streamFieldNames;
    /** Properties in the dimension node to update or retrieve */
    @Injection( name = "NODE_PROPERTY_NAME", group = "NODE_PROPS" )
    private String[] nodePropNames;
    @Injection( name = "NODE_PROPERTY_TYPE", group = "NODE_PROPS" )
    private String[] nodePropTypes;
    @Injection( name = "NODE_PROPERTY_PRIMARY", group = "NODE_PROPS" )
    private boolean[] nodePropPrimary;
    /** The type of update to perform on the properties: insert, update, punch-through */
    @Injection( name = "NODE_PROPERTY_UPDATE_TYPE", group = "NODE_PROPS" )
    private PropUpdateType[] nodePropUpdateType;
    /** The type of return field for looking up */
    @Injection( name = "TYPE_OF_RETURN_FIELD", group = "NODE_PROPS" )
    private int[] returnType;
    /** The name of node property indicating the linked node version */
    @Injection( name = "VERSION_PROP", group = "NODE_PROPS" )
    private String versionProp;
    /** The name of node property indicating the start date of dimension time range*/
    @Injection( name = "START_DATE_PROP", group = "NODE_PROPS" )
    private String startDateProp;
    /** The name of node property indicating the end date of dimension time range*/
    @Injection( name = "END_DATE_PROP", group = "NODE_PROPS" )
    private String endDateProp;
    /** Label value of the technical key (surrogate key) assigned to the dimension relationship when creating child node */
    @Injection( name = "REL_LABEL_VALUE", group = "REL_LABELS" )
    private String relLabelValue;
    /** Output name of the technical key field, the node id is used as technical key */
    @Injection( name = "TECHNICAL_KEY_PROPERTY_OUTPUT_NAME" )
    private String idRename;
    /** The field in stream to use for date range lookup in the dimension graph */
    @Injection( name = "STREAM_DATE_FIELD")
    private String streamDateField;
    /** The year to use as minus infinity on the dimension date range */
    @Injection( name = "MIN_YEAR")
    private int minYear;
    /** The year to use as plus infinity in the dimensions date range */
    @Injection( name = "MAX_YEAR")
    private int maxYear;
    /** Flag to indicate we're going to use an alternative start date */
    @Injection( name = "USE_ALTERNATIVE_START_DATE")
    private boolean usingStartDateAlternative;
    /** The type of alternative */
    @Injection( name = "ALTERNATIVE_START_OPTION")
    private StartDateAlternativeType startDateAlternativeType;
    /** The field name in case we select the property value option as an alternative start date */
    @Injection( name = "ALTERNATIVE_START_FIELD")
    private String startDateFieldName;


    public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp ) {
        return new DimensionGraph( stepMeta, stepDataInterface, cnr, transMeta, disp );
    }

    public StepDataInterface getStepData( ) {
        return new DimensionGraphData();
    }

    public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
        return new DimensionGraphDialog( shell, meta, transMeta, name );
    }

    public void setDefault( ) {
        connection = "";
        batchSize = "100";
        update = true;

        dimensionNodeLabels = new String[0];
        dimensionNodeLabelValues = new String[0];
        keyFieldsStream = new String[0];
        keyPropsLookup = new String[0];
        keyPropsLookupType = new String[0];
        streamFieldNames = new String[0];
        nodePropNames = new String[0];
        nodePropTypes = new String[0];
        nodePropPrimary = new boolean[0];
        nodePropUpdateType = new PropUpdateType[0];
        returnType = new int[0];

        relLabelValue = "DIMENSION_UPDATE";
        startDateProp = "validDateFrom";
        endDateProp = "validDateTo";
        versionProp = "version";

        idRename = "dimensionNodeID";
        streamDateField = "";
        minYear = Const.MIN_YEAR;
        maxYear = Const.MAX_YEAR;
        usingStartDateAlternative = false;
        startDateAlternativeType = StartDateAlternativeType.NONE;
        startDateFieldName = "";
    }

    @Override
    public String getXML() throws KettleException {
        normalizeAllocationFields();
        StringBuffer xml = new StringBuffer();

        xml.append( XMLHandler.addTagValue( STRING_CONNECTION, connection ) );
        xml.append( XMLHandler.addTagValue( STRING_BATCH_SIZE, batchSize ) );
        xml.append( XMLHandler.addTagValue( STRING_UPDATE, update ) );

        xml.append( XMLHandler.openTag( STRING_LABELS ) );
        for ( int i = 0; i < dimensionNodeLabels.length; i++ ) {
            xml.append( XMLHandler.addTagValue( STRING_LABEL, dimensionNodeLabels[ i ] ) );
            xml.append( XMLHandler.addTagValue( STRING_LABEL_VALUE, dimensionNodeLabelValues[ i ] ) );
        }
        xml.append( XMLHandler.closeTag( STRING_LABELS ) );

        xml.append( XMLHandler.openTag( STRING_KEYS ) );
        for ( int i = 0; i < keyFieldsStream.length; i++ ) {
            xml.append( XMLHandler.addTagValue( STRING_FIELD, keyFieldsStream[ i ] ) );
            xml.append( XMLHandler.addTagValue( STRING_LOOKUP, keyPropsLookup[ i ] ) );
            xml.append( XMLHandler.addTagValue( STRING_LOOKUP_TYPE, keyPropsLookupType[ i ] ) );
        }
        xml.append( XMLHandler.closeTag( STRING_KEYS ) );

        xml.append( XMLHandler.openTag( STRING_PROPERTIES ) );
        for ( int i = 0; i < streamFieldNames.length; i++ ) {
            xml.append( XMLHandler.openTag( STRING_PROPERTY ) );
            xml.append( XMLHandler.addTagValue( STRING_FIELD, streamFieldNames[ i ] ) );
            xml.append( XMLHandler.addTagValue( STRING_PROPERTY_NAME, nodePropNames[ i ] ) );
            xml.append( XMLHandler.addTagValue( STRING_PROPERTY_TYPE, nodePropTypes[ i ] ) );
            //if(update) {
                xml.append(XMLHandler.addTagValue(STRING_PROPERTY_PRIMARY, nodePropPrimary[i]));
                xml.append(XMLHandler.addTagValue(STRING_PROPERTY_UPDATE_TYPE, nodePropUpdateType[i].name() ) );
            //} else {
                xml.append(XMLHandler.addTagValue(STRING_FIELD_RETURE_TYPE, ValueMetaFactory.getValueMetaName( returnType[ i ] ) ) );
            //}
            xml.append( XMLHandler.closeTag( STRING_PROPERTY ) );
        }
        xml.append( XMLHandler.closeTag( STRING_PROPERTIES ) );

        xml.append( XMLHandler.openTag( STRING_RELATIONSHIP ) );
            xml.append( XMLHandler.addTagValue( STRING_REL_LABEL, relLabelValue) );
        xml.append( XMLHandler.closeTag( STRING_RELATIONSHIP ) );

        xml.append(XMLHandler.addTagValue( STRING_ID_RENAME, idRename ) );
        xml.append(XMLHandler.addTagValue( STRING_STREAM_DATE_FIELD, streamDateField ) );
        xml.append(XMLHandler.addTagValue( STRING_VERSION_PROP, versionProp) );
        xml.append(XMLHandler.addTagValue( STRING_START_DATE_PROP, startDateProp) );
        xml.append(XMLHandler.addTagValue( STRING_END_DATE_PROP, endDateProp) );
        xml.append(XMLHandler.addTagValue( STRING_MIN_YEAR, minYear ) );
        xml.append(XMLHandler.addTagValue( STRING_MAX_YEAR, maxYear ) );
        xml.append(XMLHandler.addTagValue( STRING_USE_START_DATE_ALTERNATIVE, usingStartDateAlternative ) );
        xml.append(XMLHandler.addTagValue( STRING_START_DATE_ALTERNATIVE_TYPE, startDateAlternativeType.name() ) );
        xml.append(XMLHandler.addTagValue(STRING_START_DATE_FIELD_NAME, startDateFieldName) );

        return xml.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
        connection = XMLHandler.getTagValue( stepnode, STRING_CONNECTION );
        batchSize = XMLHandler.getTagValue( stepnode, STRING_BATCH_SIZE );
        update = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_UPDATE ) );

        Node labelsNode = XMLHandler.getSubNode( stepnode, STRING_LABELS );
        List<Node> dimensionLableNodes = XMLHandler.getNodes( labelsNode, STRING_LABEL );
        List<Node> dimensionLableValueNodes = XMLHandler.getNodes( labelsNode, STRING_LABEL_VALUE );
        dimensionNodeLabels = new String[ dimensionLableNodes.size() ];
        dimensionNodeLabelValues = new String[ Math.max(dimensionLableNodes.size(), dimensionLableValueNodes.size()) ];
        for ( int i = 0; i < dimensionLableNodes.size(); i++ ) {
            Node labelNode = dimensionLableNodes.get( i );
            dimensionNodeLabels[ i ] = XMLHandler.getNodeValue( labelNode );
        }
        for ( int i = 0; i < dimensionLableValueNodes.size(); i++ ) {
            Node valueNode = dimensionLableValueNodes.get( i );
            dimensionNodeLabelValues[ i ] = XMLHandler.getNodeValue( valueNode );
        }

        Node keysNode = XMLHandler.getSubNode( stepnode, STRING_KEYS );
        List<Node> streamKeyFieldNodes = XMLHandler.getNodes( keysNode, STRING_FIELD );
        List<Node> lookupKeyPropNodes = XMLHandler.getNodes( keysNode, STRING_LOOKUP );
        List<Node> lookupKeyPropTypeNodes = XMLHandler.getNodes( keysNode, STRING_LOOKUP_TYPE );
        keyFieldsStream = new String[ streamKeyFieldNodes.size() ];
        keyPropsLookup = new String[ Math.max(streamKeyFieldNodes.size(), lookupKeyPropNodes.size()) ];
        keyPropsLookupType = new String[ Math.max(streamKeyFieldNodes.size(), lookupKeyPropNodes.size()) ];
        for ( int i = 0; i < streamKeyFieldNodes.size(); i++ ) {
            Node fieldNode = streamKeyFieldNodes.get( i );
            keyFieldsStream[ i ] = XMLHandler.getNodeValue( fieldNode );
        }
        for ( int i = 0; i < lookupKeyPropNodes.size(); i++ ) {
            Node propNode = lookupKeyPropNodes.get( i );
            keyPropsLookup[ i ] = XMLHandler.getNodeValue( propNode );
        }
        for ( int i = 0; i < lookupKeyPropTypeNodes.size(); i++ ) {
            Node propTypeNode = lookupKeyPropTypeNodes.get( i );
            keyPropsLookupType[ i ] = XMLHandler.getNodeValue( propTypeNode );
        }

        Node propsNode = XMLHandler.getSubNode( stepnode, STRING_PROPERTIES );
        List<Node> propertyNodes = XMLHandler.getNodes( propsNode, STRING_PROPERTY );
        streamFieldNames = new String[ propertyNodes.size() ];
        nodePropNames = new String[ propertyNodes.size() ];
        nodePropTypes = new String[ propertyNodes.size() ];
        nodePropPrimary = new boolean[ propertyNodes.size() ];
        nodePropUpdateType = new PropUpdateType[ propertyNodes.size() ];
        returnType = new int[ propertyNodes.size() ];
        for ( int i = 0; i < propertyNodes.size(); i++ ) {
            Node propNode = propertyNodes.get( i );
            streamFieldNames[ i ] = XMLHandler.getTagValue( propNode, STRING_FIELD );
            nodePropNames[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_NAME );
            nodePropTypes[ i ] = XMLHandler.getTagValue( propNode, STRING_PROPERTY_TYPE );
//            if( update ) {
                nodePropPrimary[ i ] = "Y".equalsIgnoreCase( XMLHandler.getTagValue( propNode, STRING_PROPERTY_PRIMARY ) );
                nodePropUpdateType[ i ] = Enum.valueOf( PropUpdateType.class, XMLHandler.getTagValue( propNode, STRING_PROPERTY_UPDATE_TYPE ) );
//            } else {
                int vmId = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( propNode, STRING_FIELD_RETURE_TYPE ) );
                if( vmId == ValueMetaInterface.TYPE_NONE ) {
                    returnType[ i ] = ValueMetaInterface.TYPE_STRING;
                }
                returnType[ i ] = vmId;
//            }
        }

        Node relNode = XMLHandler.getSubNode( stepnode, STRING_RELATIONSHIP );
        relLabelValue = XMLHandler.getTagValue( relNode, STRING_REL_LABEL);

        idRename = XMLHandler.getTagValue( stepnode, STRING_ID_RENAME);

        streamDateField = XMLHandler.getTagValue( stepnode, STRING_STREAM_DATE_FIELD);
        versionProp = XMLHandler.getTagValue( stepnode, STRING_VERSION_PROP);
        startDateProp = XMLHandler.getTagValue( stepnode, STRING_START_DATE_PROP);
        endDateProp = XMLHandler.getTagValue( stepnode, STRING_END_DATE_PROP);
        minYear = Const.toInt( XMLHandler.getTagValue( stepnode, STRING_MIN_YEAR), Const.MIN_YEAR );
        maxYear = Const.toInt( XMLHandler.getTagValue( stepnode, STRING_MAX_YEAR), Const.MAX_YEAR );
        usingStartDateAlternative = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, STRING_STREAM_DATE_FIELD ) );
        startDateAlternativeType = Enum.valueOf( StartDateAlternativeType.class, XMLHandler.getTagValue( stepnode, STRING_START_DATE_ALTERNATIVE_TYPE ) );
        startDateFieldName = XMLHandler.getTagValue( stepnode, STRING_START_DATE_FIELD_NAME);
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId ) throws KettleException {
        normalizeAllocationFields();
        try {
            rep.saveStepAttribute( transId, stepId, STRING_CONNECTION, connection );
            rep.saveStepAttribute( transId, stepId, STRING_BATCH_SIZE, batchSize );
            rep.saveStepAttribute( transId, stepId, STRING_UPDATE, update );

            String labelPrefix = STRING_LABELS + "_";
            for ( int i = 0; i < dimensionNodeLabels.length; i++ ) {
                rep.saveStepAttribute( transId, stepId, i, labelPrefix + STRING_LABEL, dimensionNodeLabels[ i ] );
                rep.saveStepAttribute( transId, stepId, i, labelPrefix + STRING_LABEL_VALUE, dimensionNodeLabelValues[ i ] );
            }

            String keyPrefix = STRING_KEYS + "_";
            for ( int i = 0; i < keyFieldsStream.length; i++ ) {
                rep.saveStepAttribute( transId, stepId, i, keyPrefix + STRING_FIELD, keyFieldsStream[ i ] );
                rep.saveStepAttribute( transId, stepId, i, keyPrefix + STRING_LOOKUP, keyPropsLookup[ i ] );
                rep.saveStepAttribute( transId, stepId, i, keyPrefix + STRING_LOOKUP_TYPE, keyPropsLookupType[ i ] );
            }

            String propPrefix = STRING_PROPERTY + "_";
            for ( int i = 0; i < streamFieldNames.length; i++ ) {
                rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_FIELD, streamFieldNames[ i ] );
                rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_PROPERTY_NAME, nodePropNames[ i ] );
                rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_PROPERTY_TYPE, nodePropTypes[ i ] );
//                if( update ) {
                    rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_PROPERTY_PRIMARY, nodePropPrimary[ i ] );
                    rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_PROPERTY_UPDATE_TYPE, nodePropUpdateType[ i ].name() );
//                } else {
                    rep.saveStepAttribute( transId, stepId, i, propPrefix + STRING_FIELD_RETURE_TYPE, ValueMetaFactory.getValueMetaName( returnType[ i ] ) );
//                }
            }

            String relPrefix = STRING_RELATIONSHIP + "_";
            rep.saveStepAttribute( transId, stepId, relPrefix + STRING_REL_LABEL, relLabelValue);

            rep.saveStepAttribute( transId, stepId, STRING_ID_RENAME, idRename );
            rep.saveStepAttribute( transId, stepId, STRING_STREAM_DATE_FIELD, streamDateField );
            rep.saveStepAttribute( transId, stepId, STRING_VERSION_PROP, versionProp);
            rep.saveStepAttribute( transId, stepId, STRING_START_DATE_PROP, startDateProp);
            rep.saveStepAttribute( transId, stepId, STRING_END_DATE_PROP, endDateProp);
            rep.saveStepAttribute( transId, stepId, STRING_MIN_YEAR, minYear );
            rep.saveStepAttribute( transId, stepId, STRING_MAX_YEAR, maxYear );
            rep.saveStepAttribute( transId, stepId, STRING_USE_START_DATE_ALTERNATIVE, usingStartDateAlternative );
            rep.saveStepAttribute( transId, stepId, STRING_START_DATE_ALTERNATIVE_TYPE, startDateAlternativeType.name() );
            rep.saveStepAttribute( transId, stepId, STRING_START_DATE_FIELD_NAME, startDateFieldName);
        } catch ( KettleDatabaseException dbe ) {
            throw new KettleException( BaseMessages.getString( PKG,
                    "Neo4jDimensionGraphMeta.Exception.UnableToSaveDimensionGraphInfoFromRepository" ), dbe );
        }
    }

    @Override
    public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
        try {
            connection = rep.getStepAttributeString( stepId, STRING_CONNECTION );
            batchSize = rep.getStepAttributeString( stepId, STRING_BATCH_SIZE );
            update = rep.getStepAttributeBoolean( stepId, STRING_UPDATE );

            String labelPrefix = STRING_LABELS + "_";
            int nrLabels = rep.countNrStepAttributes( stepId, labelPrefix + STRING_LABEL );
            dimensionNodeLabels = new String[ nrLabels ];
            dimensionNodeLabelValues = new String[ nrLabels ];
            for ( int i = 0; i < nrLabels; i++ ) {
                dimensionNodeLabels[ i ] = rep.getStepAttributeString( stepId, i, labelPrefix + STRING_LABEL );
                dimensionNodeLabelValues[ i ] = rep.getStepAttributeString( stepId, i, labelPrefix + STRING_LABEL_VALUE );
            }

            String keyPrefix = STRING_KEYS + "_";
            int nrKeys = rep.countNrStepAttributes( stepId, keyPrefix + STRING_FIELD );
            keyFieldsStream = new String[ nrKeys ];
            keyPropsLookup = new String[ nrKeys ];
            for ( int i = 0; i < nrKeys; i++ ) {
                keyFieldsStream[ i ] = rep.getStepAttributeString( stepId, i, keyPrefix + STRING_FIELD );
                keyPropsLookup[ i ] = rep.getStepAttributeString( stepId, i, keyPrefix + STRING_LOOKUP );
                keyPropsLookupType[ i ] = rep.getStepAttributeString( stepId, i, keyPrefix + STRING_LOOKUP_TYPE );
            }

            String propPrefix = STRING_PROPERTY + "_";
            int nrProps = rep.countNrStepAttributes( stepId, propPrefix + STRING_FIELD );
            streamFieldNames = new String[ nrProps ];
            nodePropNames = new String[ nrProps ];
            nodePropTypes = new String[ nrProps ];
            nodePropPrimary = new boolean[ nrProps ];
            nodePropUpdateType = new PropUpdateType[ nrProps ];
            for ( int i = 0; i < nrProps; i++ ) {
                streamFieldNames[ i ] = rep.getStepAttributeString( stepId, i, propPrefix + STRING_FIELD );
                nodePropNames[ i ] = rep.getStepAttributeString( stepId, i, propPrefix + STRING_PROPERTY_NAME );
                nodePropTypes[ i ] = rep.getStepAttributeString( stepId, i, propPrefix + STRING_PROPERTY_TYPE );
//                if( update ) {
                    nodePropPrimary[ i ] = rep.getStepAttributeBoolean( stepId, i, propPrefix + STRING_PROPERTY_PRIMARY );
                    nodePropUpdateType[ i ] = Enum.valueOf( PropUpdateType.class, rep.getStepAttributeString( stepId, i, propPrefix + STRING_PROPERTY_UPDATE_TYPE ) );
//                } else {
                    int vmId = ValueMetaFactory.getIdForValueMeta( rep.getStepAttributeString( stepId, i, propPrefix +STRING_FIELD_RETURE_TYPE ) );
                    if( vmId == ValueMetaInterface.TYPE_NONE ) {
                        returnType[ i ] = ValueMetaInterface.TYPE_STRING;
                    }
                    returnType[ i ] = vmId;
//                }
            }

            relLabelValue = rep.getStepAttributeString( stepId, STRING_RELATIONSHIP + "_" + STRING_REL_LABEL );

            idRename = rep.getStepAttributeString( stepId, STRING_ID_RENAME );
            streamDateField = rep.getStepAttributeString( stepId, STRING_STREAM_DATE_FIELD );
            versionProp = rep.getStepAttributeString( stepId, STRING_VERSION_PROP);
            startDateProp = rep.getStepAttributeString( stepId, STRING_START_DATE_PROP );
            endDateProp = rep.getStepAttributeString( stepId, STRING_END_DATE_PROP );
            minYear = (int)rep.getStepAttributeInteger( stepId, STRING_MIN_YEAR );
            maxYear = (int)rep.getStepAttributeInteger( stepId, STRING_MAX_YEAR );
            usingStartDateAlternative = rep.getStepAttributeBoolean( stepId, STRING_USE_START_DATE_ALTERNATIVE );
            startDateAlternativeType = Enum.valueOf( StartDateAlternativeType.class, rep.getStepAttributeString( stepId, STRING_START_DATE_ALTERNATIVE_TYPE ) );
            startDateFieldName = rep.getStepAttributeString( stepId, STRING_START_DATE_FIELD_NAME);

        } catch ( Exception e ) {
            throw new KettleException( BaseMessages.getString( PKG,
                    "Neo4jDimensionGraphMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository" ), e );
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input,
                                String[] output, RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore ) {
        if ( update ) {
            checkUpdate( remarks, stepMeta, prev );
        } else {
            checkLookup( remarks, stepMeta, prev );
        }
        CheckResult cr;
        if( prev == null || prev.size() == 0 ) {
            cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING,
                    BaseMessages.getString( PKG, "Neo4jDimensionGraphMeta.CheckResult.NoPreviousInfoWarning" ), stepMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( CheckResult.TYPE_RESULT_OK,
                    BaseMessages.getString( PKG, "Neo4jDimensionGraphMeta.CheckResult.PreviousInfoOK" ) + " : " + prev.size() + " fields", stepMeta );
            remarks.add( cr );
        }

        // See if we have input streams leading to this step!
        if ( input.length > 0 ) {
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
                            BaseMessages.getString( PKG, "Neo4jDimensionGraphMeta.CheckResult.StepReceiveInfoOK" ), stepMeta );
            remarks.add( cr );
        } else {
            cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString( PKG, "Neo4jDimensionGraphMeta.CheckResult.NoInputReceiveFromOtherSteps" ), stepMeta );
            remarks.add( cr );
        }

    }

    /**
     * TODO:
     * 1. need future implementation
     */
    private void checkUpdate( List<CheckResultInterface> remarks, StepMeta stepinfo, RowMetaInterface prev ) {

    }
    /**
     * TODO:
     * 1. need future implementation
     */
    private void checkLookup( List<CheckResultInterface> remarks, StepMeta stepinfo, RowMetaInterface prev ) {

    }


    @Override
    public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                     Repository repository, IMetaStore metaStore ) throws KettleStepException {
        // Change all the fields to normal storage, this is the fastest way to handle lazy conversion.
        // It doesn't make sense to use it in the SCD context but people try it anyway
        //
        for ( ValueMetaInterface valueMeta : inputRowMeta.getValueMetaList() ) {
            valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
            // Also change the trim type to "None" as this can cause trouble
            // during compare of the data when there are leading/trailing spaces in the target table
            //
            valueMeta.setTrimType( ValueMetaInterface.TRIM_TYPE_NONE );
        }

        // the output name of node id (technical key) can't be null, it should be added to the output stream
        if ( Utils.isEmpty( idRename ) ) {
            String message = BaseMessages.getString( PKG, "Neo4jDimensionGraphMeta.Error.OutputNameOfTechnicalKeyNotSpecified" );
            logError( message );
            throw new KettleStepException( message );
        }
        ValueMetaInterface v = new ValueMetaInteger( idRename, 15, 0 );
        v.setOrigin( name );
        inputRowMeta.addValueMeta( v );

        // retrieve extra fields on lookup?
        // Don't bother if there are no return values specified.
        /**
         * TODO:
         * 1. the meta info of return field should be retieved & checked within the graph database
         */
        if( !update && nodePropNames.length > 0 ) {
                for( int i = 0; i < nodePropNames.length; i++ ) {
                    try {
                        v = ValueMetaFactory.createValueMeta( nodePropNames[ i ], returnType[ i ] );
                        // If the field needs to be renamed, rename
                        if( streamFieldNames[ i ] != null && streamFieldNames[ i ].length() > 0 ) {
                            v.setName( streamFieldNames[ i ] );
                        }
                        v.setOrigin( name );
                        inputRowMeta.addValueMeta( v );
                    } catch ( KettlePluginException e ) {
                        throw new KettleStepException( "Unknown data type '" + returnType[ i ] + "' for value named '" + nodePropNames[ i ] + "'" );
                    }
                }
        }
    }

    @Override
    public Object clone() {
        return super.clone();
    }


    public Date getMinDate() {
        Calendar mincal = Calendar.getInstance();
        mincal.set( Calendar.YEAR, minYear );
        mincal.set( Calendar.MONTH, 0 );
        mincal.set( Calendar.DAY_OF_MONTH, 1 );
        mincal.set( Calendar.HOUR_OF_DAY, 0 );
        mincal.set( Calendar.MINUTE, 0 );
        mincal.set( Calendar.SECOND, 0 );
        mincal.set( Calendar.MILLISECOND, 0 );

        return mincal.getTime();
    }

    public Date getMaxDate() {
        Calendar mincal = Calendar.getInstance();
        mincal.set( Calendar.YEAR, Const.MAX_YEAR );
        mincal.set( Calendar.MONTH, 11 );
        mincal.set( Calendar.DAY_OF_MONTH, 31 );
        mincal.set( Calendar.HOUR_OF_DAY, 23 );
        mincal.set( Calendar.MINUTE, 59 );
        mincal.set( Calendar.SECOND, 59 );
        mincal.set( Calendar.MILLISECOND, 999 );

        return mincal.getTime();
    }

    public void normalizeAllocationFields() {
        if ( dimensionNodeLabels != null || dimensionNodeLabelValues != null ) {
            int labelsGroupSize = Math.max(dimensionNodeLabels.length, dimensionNodeLabelValues.length );
            if( labelsGroupSize > dimensionNodeLabels.length ) {
                dimensionNodeLabels = normalizeAllocation( dimensionNodeLabels, labelsGroupSize );
            } else {
                dimensionNodeLabelValues = normalizeAllocation( dimensionNodeLabelValues, labelsGroupSize );
            }
        }
        if ( keyPropsLookup != null ) {
            int keysGroupSize = keyPropsLookup.length;
            keyFieldsStream = normalizeAllocation( keyFieldsStream, keysGroupSize );
            keyPropsLookupType = normalizeAllocation( keyPropsLookupType, keysGroupSize );
        }
        if ( nodePropNames != null ) {
            int fieldsGroupSize = nodePropNames.length;
            streamFieldNames = normalizeAllocation( streamFieldNames, fieldsGroupSize );
            nodePropTypes = normalizeAllocation( nodePropTypes, fieldsGroupSize );
            nodePropPrimary = normalizeAllocation( nodePropPrimary, fieldsGroupSize );
            nodePropUpdateType = normalizeAllocation( nodePropUpdateType, fieldsGroupSize );
            returnType = normalizeAllocation( returnType, fieldsGroupSize );
        }
    }

    private String[] normalizeAllocation( String[] oldAllocation, int length ) {
        String[] newAllocation = null;
        if ( oldAllocation.length < length ) {
            newAllocation = new String[length];
            for ( int i = 0; i < oldAllocation.length; i++ ) {
                newAllocation[i] = oldAllocation[i];
            }
        } else {
            newAllocation = oldAllocation;
        }
        return newAllocation;
    }

    private int[] normalizeAllocation( int[] oldAllocation, int length ) {
        int[] newAllocation = null;
        if ( oldAllocation.length < length ) {
            newAllocation = new int[length];
            for ( int i = 0; i < oldAllocation.length; i++ ) {
                newAllocation[i] = oldAllocation[i];
            }
        } else {
            newAllocation = oldAllocation;
        }
        return newAllocation;
    }

    private boolean[] normalizeAllocation( boolean[] oldAllocation, int length ) {
        boolean[] newAllocation = null;
        if ( oldAllocation.length < length ) {
            newAllocation = new boolean[length];
            for ( int i = 0; i < oldAllocation.length; i++ ) {
                newAllocation[i] = oldAllocation[i];
            }
        } else {
            newAllocation = oldAllocation;
        }
        return newAllocation;
    }

    private PropUpdateType[] normalizeAllocation( PropUpdateType[] oldAllocation, int length ) {
        PropUpdateType[] newAllocation = null;
        if ( oldAllocation.length < length ) {
            newAllocation = new PropUpdateType[length];
            for ( int i = 0; i < oldAllocation.length; i++ ) {
                newAllocation[i] = oldAllocation[i];
            }
        } else {
            newAllocation = oldAllocation;
        }
        return newAllocation;
    }

    public static final boolean isUpdateTypeWithoutArgument( boolean update, PropUpdateType type ) {
        if ( !update ) {
            return false; // doesn't apply
        }
        switch ( type ) {
            case DATE_INSERTED_UPDATED:
            case DATE_INSERTED:
            case DATE_UPDATED:
            case LAST_VERSION:
                return true;
            default:
                return false;
        }
    }

    public static final StartDateAlternativeType getStartDateAlternativeType( String aString ) {
        for ( StartDateAlternativeType startType : StartDateAlternativeType.values() ) {
            if( startType.name().equalsIgnoreCase( aString ) ) {
                return startType;
            }
        }
        for ( StartDateAlternativeType startType : StartDateAlternativeType.values() ) {
            if( startType.getDesc().equalsIgnoreCase( aString ) ) {
                return startType;
            }
        }
        return StartDateAlternativeType.NONE;
    }

    public static final PropUpdateType getPropUpdateType( String aString ) {
        for ( PropUpdateType propUpdateType : PropUpdateType.values() ) {
            if( propUpdateType.name().equalsIgnoreCase( aString ) ) {
                return propUpdateType;
            }
        }
        for ( PropUpdateType propUpdateType : PropUpdateType.values() ) {
            if( propUpdateType.getDesc().equalsIgnoreCase( aString ) ) {
                return propUpdateType;
            }
        }
        return PropUpdateType.INSERT;
    }


    ////////////////////
    // getter & setter
    ////////////////////

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isUpdate() {
        return update;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public String[] getDimensionNodeLabels() {
        return dimensionNodeLabels;
    }

    public void setDimensionNodeLabels(String[] dimensionNodeLabels) {
        this.dimensionNodeLabels = dimensionNodeLabels;
    }

    public String[] getDimensionNodeLabelValues() {
        return dimensionNodeLabelValues;
    }

    public void setDimensionNodeLabelValues(String[] dimensionNodeLabelValues) {
        this.dimensionNodeLabelValues = dimensionNodeLabelValues;
    }

    public String[] getKeyFieldsStream() {
        return keyFieldsStream;
    }

    public void setKeyFieldsStream(String[] keyFieldsStream) {
        this.keyFieldsStream = keyFieldsStream;
    }

    public String[] getKeyPropsLookup() {
        return keyPropsLookup;
    }

    public void setKeyPropsLookup(String[] keyPropsLookup) {
        this.keyPropsLookup = keyPropsLookup;
    }

    public String[] getKeyPropsLookupType() {
        return keyPropsLookupType;
    }

    public void setKeyPropsLookupType(String[] keyPropsLookupType) {
        this.keyPropsLookupType = keyPropsLookupType;
    }

    public String[] getStreamFieldNames() {
        return streamFieldNames;
    }

    public void setStreamFieldNames(String[] streamFieldNames) {
        this.streamFieldNames = streamFieldNames;
    }

    public String[] getNodePropNames() {
        return nodePropNames;
    }

    public void setNodePropNames(String[] nodePropNames) {
        this.nodePropNames = nodePropNames;
    }

    public String[] getNodePropTypes() {
        return nodePropTypes;
    }

    public void setNodePropTypes(String[] nodePropTypes) {
        this.nodePropTypes = nodePropTypes;
    }

    public boolean[] getNodePropPrimary() {
        return nodePropPrimary;
    }

    public void setNodePropPrimary(boolean[] nodePropPrimary) {
        this.nodePropPrimary = nodePropPrimary;
    }

    public PropUpdateType[] getNodePropUpdateType() {
        return nodePropUpdateType;
    }

    public void setNodePropUpdateType(PropUpdateType[] nodePropUpdateType) {
        this.nodePropUpdateType = nodePropUpdateType;
    }

    public int[] getReturnType() {
        return returnType;
    }

    public void setReturnType(int[] returnType) {
        this.returnType = returnType;
    }

    public String getRelLabelValue() {
        return relLabelValue;
    }

    public void setRelLabelValue(String relLabelValue) {
        this.relLabelValue = relLabelValue;
    }

    public String getStartDateProp() {
        return startDateProp;
    }

    public void setStartDateProp(String startDateProp) {
        this.startDateProp = startDateProp;
    }

    public String getEndDateProp() {
        return endDateProp;
    }

    public void setEndDateProp(String endDateProp) {
        this.endDateProp = endDateProp;
    }

    public String getVersionProp() {
        return versionProp;
    }

    public void setVersionProp(String versionProp) {
        this.versionProp = versionProp;
    }

    public String getIdRename() {
        return idRename;
    }

    public void setIdRename(String idRename) {
        this.idRename = idRename;
    }

    public String getStreamDateField() {
        return streamDateField;
    }

    public void setStreamDateField(String streamDateField) {
        this.streamDateField = streamDateField;
    }

    public int getMinYear() {
        return minYear;
    }

    public void setMinYear(int minYear) {
        this.minYear = minYear;
    }

    public int getMaxYear() {
        return maxYear;
    }

    public void setMaxYear(int maxYear) {
        this.maxYear = maxYear;
    }

    public boolean isUsingStartDateAlternative() {
        return usingStartDateAlternative;
    }

    public void setUsingStartDateAlternative(boolean usingStartDateAlternative) {
        this.usingStartDateAlternative = usingStartDateAlternative;
    }

    public StartDateAlternativeType getStartDateAlternativeType() {
        return startDateAlternativeType;
    }

    public void setStartDateAlternativeType(StartDateAlternativeType startDateAlternativeType) {
        this.startDateAlternativeType = startDateAlternativeType;
    }

    public String getStartDateFieldName() {
        return startDateFieldName;
    }

    public void setStartDateFieldName(String startDateFieldName) {
        this.startDateFieldName = startDateFieldName;
    }

    public boolean isPreloadingCache() {
        return false;
    }
}
