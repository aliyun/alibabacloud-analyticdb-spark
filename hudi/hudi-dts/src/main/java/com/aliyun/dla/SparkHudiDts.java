package com.aliyun.dla;

import com.alibaba.dts.formats.avro.Field;
import com.alibaba.dts.formats.avro.Operation;
import com.alibaba.dts.formats.avro.Record;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.dla.hudi.DLASyncTool;
import common.FieldEntryHolder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import recordprocessor.AvroDeserializer;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.DataSourceWriteOptions.TABLE_TYPE_OPT_KEY;

public class SparkHudiDts {
    private static final Logger LOG = LogManager.getLogger(SparkHudiDts.class);
    private static int META_LENGTH = 6;
    private static int SOURCE_TIMESTAMP_INDEX = 0;
    private static int OPERATION_INDEX = 1;
    private static int DBNAME_INDEX = 2;
    private static int TABLENAME_INDEX = 3;
    private static int TBL_PKS_INDEX = 4;
    private static int _HOODIE_IS_DELETED_INDEX = 5;

    private static String SOURCE_TIMESTAMP = "sourceTimestamp";
    private static String OPERATION = "operation";
    private static String DBNAME = "dbName";
    private static String TABLENAME = "tableName";
    private static String TBL_PKS = "tbl_pks";
    private static String _HOODIE_IS_DELETED = "_hoodie_is_deleted";

    private static String DTS_USERNAME = "dtsUsername";
    private static String DTS_PASSWORD = "dtsPassword";
    private static String OFFSET = "offset";
    private static String GROUPID = "groupId";
    private static String BOOTSTRAP_SERVER = "bootstrapServer";
    private static String MAX_OFFSET_PER_TRIGGER = "maxOffsetsPerTrigger";
    private static String SUBSCRIBE_TOPIC = "subscribeTopic";

    private static String BATH_PATH = "basePath";
    private static String HOODIE_COMPACT_INLINE = "hoodie.compact.inline";
    private static String HOODIE_COMPACT_INLINE_MAX_DELTA_COMMITS = "hoodie.compact.inline.max.delta.commits";
    private static String HOODIE_TABLE_TYPE = "hoodie.table.type";
    private static String HOODIE_ENABLE_TIMELINE_SERVER = "hoodie.enable.timeline.server";
    private static String HOODIE_INSERT_SHUFFLE_PARALLELISM = "hoodie.insert.shuffle.parallelism";
    private static String HOODIE_UPSERT_SHUFFLE_PARALLELISM = "hoodie.upsert.shuffle.parallelism";

    private static String ENABLE_SYNC_TO_DLA = "enable.sync.to.dla";
    private static String DLA_USERNAME = "dla.username";
    private static String DLA_PASSWORD = "dla.password";
    private static String DLA_JDBC_URL = "dla.jdbc.url";

    private static Map<String, Map<String, HoodieWriteClient>> maps = new HashMap<>();

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkHudiDtsDemo")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[2]")
                .getOrCreate();

        String propertiesFilePath = "/tmp/config.properties";
        if (args.length != 0) {
            LOG.info("propertiesFilePath = " + args[0]);
            propertiesFilePath = args[0];
        }

        Properties properties = new Properties();
        try {
            InputStream inStream = new FileInputStream(new File(propertiesFilePath));
            properties.load(inStream);
        } catch (Exception e) {
            LOG.warn("load " + propertiesFilePath, e);
        }

        LOG.info("properties = " + properties);

        // hudi config
        String basePath = getConfig(properties, BATH_PATH, "/tmp/hudi_dts_demo/");
        Boolean inlineCompaction = Boolean.parseBoolean(getConfig(properties, HOODIE_COMPACT_INLINE, "true"));
        int maxDeltaCommits = Integer.parseInt(getConfig(properties, HOODIE_COMPACT_INLINE_MAX_DELTA_COMMITS, "10"));
        String tableType = getConfig(properties, HOODIE_TABLE_TYPE, "MERGE_ON_READ");
        int insertParall = Integer.parseInt(getConfig(properties, HOODIE_INSERT_SHUFFLE_PARALLELISM, "3"));
        int upsertParall = Integer.parseInt(getConfig(properties, HOODIE_UPSERT_SHUFFLE_PARALLELISM, "3"));
        boolean timelineServerEnable = Boolean.parseBoolean(getConfig(properties, HOODIE_ENABLE_TIMELINE_SERVER, "false"));

        // dla config
        boolean syncToDLA = Boolean.parseBoolean(getConfig(properties, ENABLE_SYNC_TO_DLA, "true"));
        String dlaUsername = getConfig(properties, DLA_USERNAME, null);
        String dlaPassword = getConfig(properties, DLA_PASSWORD, null);
        String dlaJdbcUrl = getConfig(properties, DLA_JDBC_URL, null);

        // dts config
        String username = getConfig(properties, DTS_USERNAME, null);
        String password = getConfig(properties, DTS_PASSWORD, null);
        String groupID = getConfig(properties, GROUPID, null);
        String sidName = groupID;
        String bootstrapServer = getConfig(properties, BOOTSTRAP_SERVER, null);
        String maxOffsetPerTrigger = getConfig(properties, MAX_OFFSET_PER_TRIGGER, "10000");
        String subscribeTopic = getConfig(properties, SUBSCRIBE_TOPIC, null);
        String offset = getConfig(properties, OFFSET, null);
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s-%s\" password=\"%s\";";
        String kafkaParamPrefix = "kafka.";
        String offsets = "{\"" + subscribeTopic + "\":{\"0\":" + offset + "}}";
        Dataset<Row> dataset = spark.readStream()
                .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
                .option(kafkaParamPrefix + SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, username, sidName, password))
                .option(kafkaParamPrefix + SaslConfigs.SASL_MECHANISM, "PLAIN")
                .option(kafkaParamPrefix + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                .option(kafkaParamPrefix + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                .option("startingOffsets", offsets)
                .option("maxOffsetsPerTrigger", maxOffsetPerTrigger)
                .option("subscribe", subscribeTopic)
                .load();

        StreamingQuery result = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
            LOG.info("batchID => " + aLong);

            Dataset<Row> rows = rowDataset.select("value");

            Dataset<Row> upsertDF = rows.filter((FilterFunction<Row>) row -> {
                Record record = new AvroDeserializer().deserialize((byte[])row.get(0));
                Operation operation = record.getOperation();
                return operation.equals(Operation.INSERT) ||
                        operation.equals(Operation.UPDATE) ||
                        operation.equals(Operation.DELETE);
            });

            JavaRDD<Row> upsertRDD = upsertDF.toJavaRDD().map((Function<Row, Row>) row -> {
                Record record = new AvroDeserializer().deserialize((byte[])row.get(0));
                StringBuilder sb = new StringBuilder();
                JSONArray jsonArray = JSONArray.parseArray(JSONObject.parseObject(record.getTags().get("pk_uk_info")).getString("PRIMARY"));
                for (int i = 0; i < jsonArray.size(); i++) {
                    sb.append(jsonArray.getString(i)).append(",");
                }
                String pks = sb.deleteCharAt(sb.toString().length() - 1).toString();
                Object[] objects = resolveRecordSchema(record);
                String dbName = (String) objects[0];
                String tableName = (String) objects[1];
                StructType structType = (StructType) objects[2];

                List<Object> array = new ArrayList();
                array.add(record.getSourceTimestamp() * 1000);
                array.add(record.getOperation().toString());
                array.add(dbName);
                array.add(tableName);
                array.add(pks);

                List<Field> fields = (List<Field>) record.getFields();
                FieldEntryHolder fieldArrayBefore = new FieldEntryHolder((List<Object>) record.getBeforeImages());
                FieldEntryHolder fieldArrayAfter = new FieldEntryHolder((List<Object>) record.getAfterImages());
                Operation operation = record.getOperation();
                Iterator<Field> fieldIterator = fields.iterator();
                boolean init = false;
                while (fieldIterator.hasNext() && fieldArrayAfter.hasNext() && fieldArrayBefore.hasNext()) {
                    Field field = fieldIterator.next();
                    Object afterValue = fieldArrayAfter.take();
                    Object beforeValue = fieldArrayBefore.take();
                    if (operation != Operation.DELETE) {
                        if (!init) {
                            array.add(false);
                            init = true;
                        }
                        array.add(convertDtsValueToDataType(field.getDataTypeNumber(), afterValue));
                    } else {
                        if (!init) {
                            array.add(true);
                            init = true;
                        }
                        array.add(convertDtsValueToDataType(field.getDataTypeNumber(), beforeValue));
                    }
                }

                Object[] objectArray = array.toArray(new Object[0]);
                Row r = (new GenericRowWithSchema(objectArray, structType));
                return r;
            });
            // group by database name and table name
            JavaPairRDD<Tuple2<String, String>, Iterable<Row>> groupbyRDD = upsertRDD.groupBy((Function<Row, Tuple2<String, String>>) v1 -> {
                String dbName = v1.getString(DBNAME_INDEX);
                String tableName = v1.getString(TABLENAME_INDEX);
                return new Tuple2<>(dbName, tableName);
            });
            // collect all results.
            Map<Tuple2<String, String>, Iterable<Row>> collectAsMap = groupbyRDD.collectAsMap();

            Map<String, List<String>> db2Tables = new HashMap<>();
            for (Map.Entry<Tuple2<String, String>, Iterable<Row>> entry : collectAsMap.entrySet()) {
                Tuple2<String, String> db2Table = entry.getKey();
                String dbName = db2Table._1;
                String tableName = db2Table._2;
                if (!db2Tables.containsKey(dbName)) {
                    db2Tables.put(dbName, new ArrayList<>());
                }
                db2Tables.get(dbName).add(tableName);

                Iterator<Row> rowsIterator = entry.getValue().iterator();
                Schema schema = null;
                boolean init = false;
                List<HoodieRecord> lists = new ArrayList<>();
                while (rowsIterator.hasNext()) {
                    Row row = rowsIterator.next();
                    String structName = tableName + "_record";
                    String nameSpace = "hoodie." + tableName;
                    if (!init) {
                        StructType originSchema = addOperationField(getOriginStructType(row.schema()));
                        schema = AvroConversionUtils.convertStructTypeToAvroSchema(originSchema, structName, nameSpace);
                        init = true;
                    }

                    HoodieRecord record = convertRow2Record(row, structName, nameSpace);
                    lists.add(record);
                }


                if (!maps.containsKey(dbName)) {
                    maps.put(dbName, new HashMap<>());
                }

                String tableBasePath = basePath + dbName + "/" + tableName;
                if (!maps.get(dbName).containsKey(tableName)) {
                    HoodieWriteConfig hoodieWriteConfig = new HoodieWriteConfig.Builder()
                            .withSchema(schema.toString())
                            .forTable(tableName)
                            .withCompactionConfig(new HoodieCompactionConfig.Builder().withInlineCompaction(inlineCompaction).withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits).build())
                            .withEmbeddedTimelineServerEnabled(timelineServerEnable)
                            .withParallelism(insertParall, upsertParall)
                            .withPath(tableBasePath)
                            .build();
                    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
                    init(jsc.hadoopConfiguration(), tableName, tableBasePath, HoodieTableType.valueOf(tableType));
                    HoodieWriteClient client = new HoodieWriteClient(jsc, hoodieWriteConfig);
                    maps.get(dbName).put(tableName, client);
                }
                HoodieWriteClient client = maps.get(dbName).get(tableName);
                JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
                JavaRDD<HoodieRecord> rowJavaRDD = jsc.parallelize(lists, 1);
                String time = client.startCommit();
                try {
                    JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(rowJavaRDD, time);
                    List<Boolean> errors = writeStatusJavaRDD.map(writeStatus -> writeStatus.hasErrors()).filter(s -> s).collect();
                    if (errors.isEmpty()) {
                        LOG.info("write success...");
                    } else {
                        LOG.warn("write failed...");
                    }
                } catch (Exception e) {
                    LOG.error(e);
                } finally {
                    lists.clear();
                }

                if (syncToDLA) {
                    Properties dlaProperites = new Properties();
                    dlaProperites.put("basePath", tableBasePath);
                    dlaProperites.put("hoodie.datasource.dla_sync.database", dbName);
                    dlaProperites.put("hoodie.datasource.dla_sync.table", tableName);
                    dlaProperites.put("hoodie.datasource.dla_sync.username", dlaUsername);
                    dlaProperites.put("hoodie.datasource.dla_sync.password", dlaPassword);
                    dlaProperites.put("hoodie.datasource.dla_sync.jdbcurl", dlaJdbcUrl);
                    dlaProperites.put(TABLE_TYPE_OPT_KEY(), tableType);
                    dlaProperites.put("hoodie.datasource.dla_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor");
                    dlaProperites.put("hoodie.datasource.dla_sync.partition_fields", "");

                    FileSystem fileSystem = FSUtils.getFs(tableBasePath, jsc.hadoopConfiguration());
                    DLASyncTool dlaSyncTool = new DLASyncTool(dlaProperites, fileSystem);
                    dlaSyncTool.syncHoodieTable();
                } else {
                    LOG.info("skip sync to DLA.");
                }
            }
        }).trigger(Trigger.ProcessingTime("60 seconds")).start();
        result.awaitTermination();
    }

    public static String getConfig(Properties properties, String key, String defaultValue) {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        }
        return defaultValue;
    }

    public static HoodieTableMetaClient init(Configuration hadoopConf, String tableName, String basePath, HoodieTableType tableType)
            throws IOException {
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
        properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, OverwriteWithLatestAvroPayload.class.getName());
        properties.setProperty(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, "archived");
        return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
    }

    public static HoodieRecord convertRow2Record(Row row, String structName, String namespace) {
        String pks = row.getString(TBL_PKS_INDEX);

        long ts = Long.parseLong(row.get(SOURCE_TIMESTAMP_INDEX).toString());
        StructType type = getOriginStructType(row.schema());
        Schema schema = AvroConversionUtils.convertStructTypeToAvroSchema(type, structName, namespace);

        String[] keys = pks.split(",");
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            Schema.Field field  = schema.getField(key);
            sb.append(row.get(field.pos() + (META_LENGTH - 1))).append(",");
        }
        sb.deleteCharAt(sb.toString().length() - 1);

        List<Schema.Field> fields = schema.getFields();
        Schema avro = AvroConversionUtils.convertStructTypeToAvroSchema(addOperationField(type), structName, namespace);
        GenericRecord record = new GenericData.Record(avro);
        record.put(_HOODIE_IS_DELETED, row.getBoolean(_HOODIE_IS_DELETED_INDEX));
        for (Schema.Field field : fields) {
            String name = field.name();
            if (_HOODIE_IS_DELETED.equals(name)) {
                continue;
            }
            record.put(name, row.get(field.pos() + META_LENGTH - 1));
        }
        record.put(OPERATION, row.getString(OPERATION_INDEX));
        HoodieKey hoodieKey = new HoodieKey(sb.toString(), "");

        OverwriteWithLatestAvroPayload overwriteWithLatestAvroPayload = new OverwriteWithLatestAvroPayload(record, ts);
        HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, overwriteWithLatestAvroPayload);

        return hoodieRecord;
    }

    public static List<String> getDBTableName(String[] dbPair) {
        String dbName = "";
        String tableName = "";
        if (null != dbPair) if (dbPair.length == 2) {
            dbName = dbPair[0];
            tableName = dbPair[1];
        } else if (dbPair.length == 3) {
            dbName = dbPair[0];
            tableName = dbPair[2];
        } else if (dbPair.length == 1) {
            dbName = dbPair[0];
            tableName = "";
        }
        return Arrays.asList(dbName, tableName);
    }


    public static StructType getOriginStructType(StructType structType) {
        StructField[] fields = structType.fields();

        List<StructField> newFields = new ArrayList<>();
        for (int i = META_LENGTH - 1; i < fields.length; i++) {
            StructField field = fields[i];
            newFields.add(field);
        }
        StructType newST = new StructType(newFields.toArray(new StructField[0]));
        return newST;
    }

    public static StructType addOperationField(StructType structType) {
        StructField[] fields = structType.fields();

        List<StructField> newFields = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            StructField field = fields[i];
            newFields.add(field);
        }
        StructField field = new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty());
        newFields.add(field);
        StructType newST = new StructType(newFields.toArray(new StructField[0]));
        return newST;
    }

    public static Object convertDtsValueToDataType(int dtsTypeId, Object o) {
        switch (dtsTypeId) {
            case 1 : case 2 : case 3 :
                com.alibaba.dts.formats.avro.Integer integer = (com.alibaba.dts.formats.avro.Integer) o;
                return Integer.parseInt(integer.getValue());
            case 5 :
                com.alibaba.dts.formats.avro.Float f = (com.alibaba.dts.formats.avro.Float) o; //Type.DOUBLE
                return f.getValue();
            case 7 :
                com.alibaba.dts.formats.avro.Timestamp t = (com.alibaba.dts.formats.avro.Timestamp) o;
                return new Timestamp(t.getTimestamp());
            case 8 :
                com.alibaba.dts.formats.avro.Integer t1 = (com.alibaba.dts.formats.avro.Integer) o;
                return Integer.parseInt(t1.getValue());
            case 9 :
                com.alibaba.dts.formats.avro.Integer t2 = (com.alibaba.dts.formats.avro.Integer) o;
                return Integer.parseInt(t2.getValue());
            case 10 : {
                com.alibaba.dts.formats.avro.DateTime dataTime = (com.alibaba.dts.formats.avro.DateTime) o;
                return new Timestamp(dataTime.getYear(), dataTime.getMonth(), dataTime.getDay(),
                        dataTime.getHour(), dataTime.getMinute(), dataTime.getMillis(), 0);
            }
            case 12 : {
                com.alibaba.dts.formats.avro.DateTime dataTime = (com.alibaba.dts.formats.avro.DateTime) o;
                return new Timestamp(dataTime.getYear(), dataTime.getMonth(), dataTime.getDay(),
                        dataTime.getHour(), dataTime.getMinute(), dataTime.getMillis(), 0);
            }
            case 254 :
                com.alibaba.dts.formats.avro.Character c = (com.alibaba.dts.formats.avro.Character) o;
                return new String(c.getValue().array(), StandardCharsets.UTF_8);
            case 253 :
                com.alibaba.dts.formats.avro.Character c1 = (com.alibaba.dts.formats.avro.Character) o;
                return new String(c1.getValue().array(), StandardCharsets.UTF_8);

            default:
                return null;
        }
    }

    public static Object[] resolveRecordSchema(Record record) {
        String objectName = record.getObjectName();
        String[] dbInfo = common.Util.uncompressionObjectName(objectName);
        List<String> dbTableNames = getDBTableName(dbInfo);

        String dbName = dbTableNames.get(0);
        String tableName = dbTableNames.get(1);

        List<Field> fields = (List<Field>) record.getFields();
        FieldEntryHolder fieldEntryHolder = getFieldEntryHolder(record);
        List<StructField> schemas = new ArrayList<>();

        schemas.add(new StructField(SOURCE_TIMESTAMP, DataTypes.LongType, true, Metadata.empty()));
        schemas.add(new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()));
        schemas.add(new StructField(DBNAME, DataTypes.StringType, true, Metadata.empty()));
        schemas.add(new StructField(TABLENAME, DataTypes.StringType, true, Metadata.empty()));
        schemas.add(new StructField(TBL_PKS, DataTypes.StringType, true, Metadata.empty()));
        schemas.add(new StructField(_HOODIE_IS_DELETED, DataTypes.BooleanType, true, Metadata.empty()));

        if (null != fields) {
            Iterator<Field> fieldIterator = fields.iterator();
            while (fieldIterator.hasNext() && fieldEntryHolder.hasNext()) {
                Field field = fieldIterator.next();
                schemas.add(buildStructField(field.getDataTypeNumber(), field.getName()));
            }
        }

        StructField[] array = schemas.toArray(new StructField[0]);
        StructType schema = new StructType(array);

        Object[] objects = new Object[3];
        objects[0] = dbName;
        objects[1] = tableName;
        objects[2] = schema;

        return objects;
    }

    private static FieldEntryHolder getFieldEntryHolder(Record record) {
        return new FieldEntryHolder((List<Object>) record.getAfterImages());
    }

    private static StructField buildStructField(int dtsTypeId, String fieldName) {
        switch (dtsTypeId) {
            //              case 0 => new StructField(fieldName, DecimalType(),true); //Type.DECIMAL
            case 1 :
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()); //Type.INT8;
            case 2 :
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()); //Type.INT16;
            case 3 :
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()); //Type.INT32;
            //
            //            DATA_ADAPTER[4] = new DoubleStringAdapter(); //Type.FLOAT
            case 5 :
                return new StructField(fieldName, DataTypes.DoubleType, true, Metadata.empty()); //Type.DOUBLE
            //
            //            DATA_ADAPTER[6] = new UTF8StringEncodeAdapter(); //Type.NULL
            //
            case 7 :
                return new StructField(fieldName, DataTypes.TimestampType, true, Metadata.empty()); //Type.TIMESTAMP
            case 8 :
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()); //Type.INT64
            case 9 :
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty()); //Type.INT24
            //
            case 10 :
                return new StructField(fieldName, DataTypes.DateType, true, Metadata.empty()); //Type.DATE
            //            DATA_ADAPTER[11] = new TimeAdapter(); //Type.TIME
            case 12 :
                return new StructField(fieldName, DataTypes.TimestampType, true, Metadata.empty());
            //            DATA_ADAPTER[13] = new YearAdapter(); //Type.YEAR
            //            DATA_ADAPTER[14] = new DateTimeAdapter(); //Type.DATETIME
            //            DATA_ADAPTER[15] = new CharacterAdapter(); 	//Type.STRING
            //            DATA_ADAPTER[16] = new NumberStringAdapter(); //Type.BIT
            //
            //            DATA_ADAPTER[255] = new GeometryAdapter(); 	//Type.GEOMETRY;
            case 254 :
                return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()); //Type.STRING;
            case 253 :
                return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty()); //Type.STRING;
            //
            //            DATA_ADAPTER[252] = new BinaryAdapter(); //Type.BLOB;
            //            DATA_ADAPTER[251] = new BinaryAdapter(); //Type.BLOB;
            //            DATA_ADAPTER[250] = new BinaryAdapter(); //Type.BLOB;
            //            DATA_ADAPTER[249] = new BinaryAdapter(); //Type.BLOB;
            //
            //            DATA_ADAPTER[246] = new DecimalStringAdapter(); //Type.DECIMAL;
            //
            //            DATA_ADAPTER[248] = new TextObjectAdapter(); //Type.SET;
            //            DATA_ADAPTER[247] = new TextObjectAdapter(); //Type.ENUM;
            //            DATA_ADAPTER[245] = new TextObjectAdapter();  //Type.JSON;
            default:
                throw new RuntimeException("not supported..");
        }
    }
}
