package com.aliyun.dla;

import com.alibaba.dts.formats.avro.Field;
import com.alibaba.dts.formats.avro.Operation;
import com.alibaba.dts.formats.avro.Record;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.FieldEntryHolder;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.dla.DLASyncTool;
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
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import recordprocessor.AvroDeserializer;
import scala.Tuple2;
import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.aliyun.dla.Constants.DBNAME_INDEX;
import static com.aliyun.dla.Constants.TABLENAME_INDEX;
import static com.aliyun.dla.DtsTypeConversionUtils.buildStructField;
import static com.aliyun.dla.DtsTypeConversionUtils.convertDtsValueToDataType;
import static com.aliyun.dla.DtsTypeConversionUtils.convertStructTypeToStringType;
import static com.aliyun.dla.UserConfig.BATH_PATH;
import static com.aliyun.dla.UserConfig.CONVERT_DECIMAL_TO_STRING;
import static com.aliyun.dla.UserConfig.DECIMAL_COLUMNS_DEFINITION;
import static com.aliyun.dla.Utils.convertRow2Record;
import static com.aliyun.dla.Utils.getConfig;
import static com.aliyun.dla.Utils.getDBAndTablePair;
import static com.aliyun.dla.Utils.getOriginStructType;
import static com.aliyun.dla.UserConfig.BOOTSTRAP_SERVER;
import static com.aliyun.dla.UserConfig.CONVERT_ALL_TYPES_TO_STRING;
import static com.aliyun.dla.UserConfig.DLA_DATABASE;
import static com.aliyun.dla.UserConfig.DLA_JDBC_URL;
import static com.aliyun.dla.UserConfig.DLA_PASSWORD;
import static com.aliyun.dla.UserConfig.DLA_USERNAME;
import static com.aliyun.dla.UserConfig.DTS_PASSWORD;
import static com.aliyun.dla.UserConfig.DTS_USERNAME;
import static com.aliyun.dla.UserConfig.ENABLE_SYNC_TO_DLA;
import static com.aliyun.dla.UserConfig.FETCH_MESSAGE_MAX_BYTES;
import static com.aliyun.dla.UserConfig.GROUPID;
import static com.aliyun.dla.UserConfig.HOODIE_COMPACT_INLINE;
import static com.aliyun.dla.UserConfig.HOODIE_COMPACT_INLINE_MAX_DELTA_COMMITS;
import static com.aliyun.dla.UserConfig.HOODIE_ENABLE_TIMELINE_SERVER;
import static com.aliyun.dla.UserConfig.HOODIE_INSERT_SHUFFLE_PARALLELISM;
import static com.aliyun.dla.UserConfig.HOODIE_TABLE_TYPE;
import static com.aliyun.dla.UserConfig.HOODIE_UPSERT_SHUFFLE_PARALLELISM;
import static com.aliyun.dla.UserConfig.MAX_OFFSET_PER_TRIGGER;
import static com.aliyun.dla.UserConfig.OFFSET;
import static com.aliyun.dla.UserConfig.SUBSCRIBE_TOPIC;
import static com.aliyun.dla.Utils.getRecordNameAndNamespace;
import static com.aliyun.dla.Utils.getUserConfigDecimals;
import static org.apache.hudi.DataSourceWriteOptions.TABLE_TYPE_OPT_KEY;

public class SparkHudiDts {
    private static final Logger LOG = LogManager.getLogger(SparkHudiDts.class);

    private static boolean convertAllTypesToString = false;
    private static boolean convertDecimalToString = false;

    // 缓存HoodieWriteClient
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
            Path path = new Path(propertiesFilePath);
            FileSystem fs = path.getFileSystem(spark.sparkContext().hadoopConfiguration());
            FSDataInputStream inputStream = fs.open(path);
            properties.load(inputStream);
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
        String dlaDatabase = getConfig(properties, DLA_DATABASE, null);

        convertAllTypesToString = Boolean.parseBoolean(getConfig(properties, CONVERT_ALL_TYPES_TO_STRING, "false"));
        convertDecimalToString = Boolean.parseBoolean(getConfig(properties, CONVERT_DECIMAL_TO_STRING, "false"));

        Map<String, Pair<Integer, Integer>> decimalColumns = getUserConfigDecimals(getConfig(properties, DECIMAL_COLUMNS_DEFINITION, ""));

        // dts config
        String fetchMessageMaxBytes = getConfig(properties, FETCH_MESSAGE_MAX_BYTES, null);
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

        while (true) {
            try {
                String offsets;
                if (offset.equalsIgnoreCase("earliest") || offset.equalsIgnoreCase("latest")) {
                    offsets = offset;
                } else {
                    offsets = "{\"" + subscribeTopic + "\":{\"0\":" + offset + "}}";
                }

                DataStreamReader dataStreamReader = spark.readStream()
                        .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
                        .option(kafkaParamPrefix + SaslConfigs.SASL_JAAS_CONFIG, String.format(jaasTemplate, username, sidName, password))
                        .option(kafkaParamPrefix + SaslConfigs.SASL_MECHANISM, "PLAIN")
                        .option(kafkaParamPrefix + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                        .option(kafkaParamPrefix + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                        .option("startingOffsets", offsets)
                        .option("maxOffsetsPerTrigger", maxOffsetPerTrigger)
                        .option("subscribe", subscribeTopic);

                if (StringUtils.isNotEmpty(fetchMessageMaxBytes)) {
                    dataStreamReader.option(FETCH_MESSAGE_MAX_BYTES, fetchMessageMaxBytes);
                }

                Dataset<Row> dataset = dataStreamReader.load();
                StreamingQuery result = dataset.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
                    long startTime = System.currentTimeMillis();

                    LOG.info("batchID => " + aLong);

                    Dataset<Row> rows = rowDataset.select("value");

                    Dataset<Row> upsertDF = rows.filter((FilterFunction<Row>) row -> {
                        Record record = new AvroDeserializer().deserialize((byte[]) row.get(0));
                        Operation operation = record.getOperation();
                        // 支持三种DDL(不支持对字段的删除)
                        return operation.equals(Operation.INSERT) ||
                                operation.equals(Operation.UPDATE) ||
                                operation.equals(Operation.DELETE);
                    });

                    JavaRDD<Row> upsertRDD = upsertDF.toJavaRDD().map((Function<Row, Row>) row -> {
                        Record record = new AvroDeserializer().deserialize((byte[]) row.get(0));
                        StringBuilder sb = new StringBuilder();

                        JSONArray jsonArray = JSONArray.parseArray(JSONObject.parseObject(record.getTags().get("pk_uk_info")).getString("PRIMARY"));
                        if (jsonArray == null) { // 无主键的表
                            LOG.warn("do not have pk_uk_info, will be filtered out. record => " + record);
                            return null;
                        }

                        // 获取表的主键
                        for (int i = 0; i < jsonArray.size(); i++) {
                            sb.append(jsonArray.getString(i)).append(",");
                        }
                        String pks = sb.deleteCharAt(sb.toString().length() - 1).toString();

                        // 解析记录的库名/表名
                        Pair<String, String> dbAndTablePair = getDBAndTablePair(record);
                        String dbName = dbAndTablePair.getLeft();
                        String tableName = dbAndTablePair.getRight();

                        // 获取元数据字段列表
                        List<StructField> structFields = Utils.getMetaStructFields();

                        // 存放数据数组
                        List<Object> dataArray = new ArrayList();

                        // 添加元数据 SOURCE_TIMESTAMP/OPERATION/DBNAME/TABLENAME/TBL_PKS
                        dataArray.add(record.getSourceTimestamp() * 1000);
                        dataArray.add(record.getOperation().toString());
                        dataArray.add(dbName);
                        dataArray.add(tableName);
                        dataArray.add(pks);


                        List<Field> fields = (List<Field>) record.getFields();
                        FieldEntryHolder fieldArrayBefore = new FieldEntryHolder((List<Object>) record.getBeforeImages());
                        FieldEntryHolder fieldArrayAfter = new FieldEntryHolder((List<Object>) record.getAfterImages());
                        Operation operation = record.getOperation();
                        Iterator<Field> fieldIterator = fields.iterator();
                        // 添加_HOODIE_IS_DELETE
                        if (operation != Operation.DELETE) {
                            dataArray.add(false);
                        } else {
                            dataArray.add(true);
                        }

                        while (fieldIterator.hasNext()
                                && fieldArrayAfter.hasNext()
                                && fieldArrayBefore.hasNext()) {
                            Field field = fieldIterator.next();
                            Object afterValue = fieldArrayAfter.take();
                            Object beforeValue = fieldArrayBefore.take();

                            // 获取字段类型
                            StructField structField = buildStructField(field, field.getName(), afterValue, convertDecimalToString, decimalColumns);
                            if (convertAllTypesToString) {
                                structField = convertStructTypeToStringType(structField);
                            }
                            structFields.add(structField);

                            // 获取字段值
                            Object value = operation != Operation.DELETE ? afterValue : beforeValue;
                            Object object = convertDtsValueToDataType(structField, tableName, field, value, convertDecimalToString);
                            if (object == null) {
                                dataArray.add(null);
                            } else {
                                if (convertAllTypesToString) {
                                    String str = object.toString();
                                    dataArray.add(str);
                                } else {
                                    dataArray.add(object);
                                }
                            }
                        }

                        // 生成带元数据信息的Row
                        StructField[] schemaArray = structFields.toArray(new StructField[0]);
                        StructType structType = new StructType(schemaArray);
                        Object[] objectArray = dataArray.toArray(new Object[0]);
                        Row r = (new GenericRowWithSchema(objectArray, structType));
                        return r;
                    });

                    // 过滤掉无主键的表
                    upsertRDD = upsertRDD.filter(s -> s != null);

                    // 按照库名和表名进行groupby
                    JavaPairRDD<Tuple2<String, String>, Iterable<Row>> groupbyRDD = upsertRDD.groupBy((Function<Row, Tuple2<String, String>>) v1 -> {
                        String dbName = v1.getString(DBNAME_INDEX);
                        String tableName = v1.getString(TABLENAME_INDEX);
                        return new Tuple2<>(dbName, tableName);
                    });

                    // 汇总不同库表的记录
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

                        // 取字段最多的schema（考虑可能增加字段）
                        Optional<Row> largestRow = Lists.newArrayList(entry.getValue()).stream().sorted((o1, o2) -> o1.size() < o2.size() ? 1 : -1).findFirst();

                        Row row = largestRow.get();
                        Pair<String, String> pair = getRecordNameAndNamespace(tableName);
                        StructType originSchema = getOriginStructType(row.schema());
                        Schema schema = AvroConversionUtils.convertStructTypeToAvroSchema(originSchema, pair.getLeft(), pair.getRight());

                        // 转化为HoodieRecord
                        List<HoodieRecord> hoodieRecords = Lists.newArrayList(entry.getValue()).stream().map(row1 -> convertRow2Record(row1, schema)).collect(Collectors.toList());

                        if (!maps.containsKey(dbName)) {
                            maps.put(dbName, new HashMap<>());
                        }
                        LOG.info("writing schema = " + schema.toString());

                        // 按照用户指定basePath/dbName/tableName路径存储
                        String tableBasePath = basePath + dbName + "/" + tableName;

                        if (!maps.get(dbName).containsKey(tableName)) {
                            HoodieWriteConfig hoodieWriteConfig = new HoodieWriteConfig.Builder()
                                    .withSchema(schema.toString())
                                    .forTable(tableName)
                                    .withCompactionConfig(new HoodieCompactionConfig.Builder().withInlineCompaction(!tableType.equalsIgnoreCase("MERGE_ON_READ") ? false : inlineCompaction).withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits).build())
                                    .withEmbeddedTimelineServerEnabled(timelineServerEnable)
                                    .withParallelism(insertParall, upsertParall)
                                    .withPath(tableBasePath)
                                    .build();
                            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
                            init(jsc.hadoopConfiguration(), tableName, tableBasePath, HoodieTableType.valueOf(tableType));
                            HoodieWriteClient client = new HoodieWriteClient(jsc, hoodieWriteConfig);
                            maps.get(dbName).put(tableName, client);
                        }

                        // 获取缓存的Client
                        HoodieWriteClient client = maps.get(dbName).get(tableName);
                        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
                        JavaRDD<HoodieRecord> rowJavaRDD = jsc.parallelize(hoodieRecords, 1);
                        String time = client.startCommit();
                        try {
                            // 进行upsert操作
                            JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(rowJavaRDD, time);
                            List<Boolean> errors = writeStatusJavaRDD.map(writeStatus -> writeStatus.hasErrors()).filter(s -> s).collect();
                            if (errors.isEmpty()) {
                                LOG.info("write success...");
                            } else {
                                LOG.warn("write failed...");
                            }
                        } catch (Exception e) {
                            // Upsert失败，先不让任务失败
                            LOG.warn("upsert error " + e.getMessage(), e);
                        } finally {
                            // 清除记录，减少内存占用
                            hoodieRecords.clear();
                        }
                        if (syncToDLA) {
                            long start = System.currentTimeMillis();
                            LOG.info("begin to sync to dla");
                            try {
                                Properties dlaProperites = new Properties();
                                dlaProperites.put("basePath", tableBasePath);
                                if (StringUtils.isNotEmpty(dlaDatabase)) {
                                    dlaProperites.put("hoodie.datasource.dla_sync.database", dlaDatabase);
                                } else {
                                    dlaProperites.put("hoodie.datasource.dla_sync.database", dbName);
                                }
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
                            } catch (Exception e) {
                                LOG.warn("sync to dla error. " + e.getMessage(), e);
                            }
                            LOG.info("finish sync to dla, cost = " + (System.currentTimeMillis() - start));
                        } else {
                            LOG.info("skip sync to DLA.");
                        }
                    }
                    LOG.info("process one batch = " + maxOffsetPerTrigger + ", cost = " + (System.currentTimeMillis() - startTime) / 1000 + "s");
                }).trigger(Trigger.ProcessingTime("60 seconds")).start();

                result.awaitTermination();
            } catch (Throwable e) {
                if (e.getMessage().contains("Cannot fetch offset") || e.getMessage().contains("Offsets out of range with no configured reset policy for partitions")) {
                    LOG.warn("consume error, restart consuming from earliest." + e, e);
                    offset = "earliest";
                } else {
                    LOG.error("consume error. exit the program." + e.getMessage(), e);
                    throw e;
                }
            }
        }
    }

    public static HoodieTableMetaClient init(Configuration hadoopConf, String tableName, String basePath, HoodieTableType tableType)
            throws IOException {
        Properties properties = new Properties();
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, tableName);
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_VERSION_PROP_NAME, "1");
        properties.setProperty(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, tableType.name());
        properties.setProperty(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME, OverwriteWithLatestAvroPayload.class.getName());
        properties.setProperty(HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP_NAME, "archived");
        return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
    }
}
