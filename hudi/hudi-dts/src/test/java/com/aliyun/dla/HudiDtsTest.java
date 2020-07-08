package com.aliyun.dla;

import com.alibaba.dts.formats.avro.DateTime;
import com.alibaba.dts.formats.avro.Field;
import com.google.common.collect.Lists;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.aliyun.dla.SparkHudiDts.init;
import static com.aliyun.dla.Utils.getRecordNameAndNamespace;
import static com.aliyun.dla.Utils.getUserConfigDecimals;
import static org.apache.hudi.DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY;
import static org.apache.hudi.DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY;
import static org.apache.hudi.DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY;
import static org.apache.hudi.DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY;
import static org.apache.hudi.DataSourceWriteOptions.TABLE_TYPE_OPT_KEY;
import static org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs;
import static org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME;

import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import recordprocessor.FieldValue;
import recordprocessor.mysql.MysqlFieldConverter;

public class HudiDtsTest {
    static SparkSession spark = SparkSession
            .builder()
            .appName("SparkHudiDtsDemo")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[2]")
            .getOrCreate();

    static JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    @Test
    public void test00() {
        List<Name> integers = new ArrayList<>();
        integers.add(new Name("aa", 11));
        integers.add(new Name("bb", 15));
        integers.add(new Name("cc", 13));

        Optional<Name> in = integers.stream().sorted(new Comparator<Name>() {
            @Override
            public int compare(Name o1, Name o2) {
                return o1.age < o2.age ? 1 : -1;
            }
        }).findFirst();
        System.out.println(in.get());
    }

    @Test
    public void test2() {
        String str = String.valueOf(new Timestamp(2000 - 1900, 0, 0, 0, 0, 0, 0).getTime());
        System.out.println(str);
        DateTime dateTime = new DateTime();
        dateTime.setDay(1);
        dateTime.setHour(1);
        dateTime.setMinute(10);

        MysqlFieldConverter converter = new MysqlFieldConverter();
        Field field = new Field("aa", 10);
        Object object = new com.alibaba.dts.formats.avro.DateTime(2020, null, null, null, null, null, null);
        FieldValue value = converter.convert(field, object);
        System.out.println(new String(value.getValue()));

        converter.convert(field, new com.alibaba.dts.formats.avro.DateTime());
    }

    @Test
    public void test1() {
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"access_supplier_log_record\",\"namespace\":\"hoodie.access_supplier_log\",\"fields\":[{\"name\":\"_hoodie_is_deleted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"asl_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"user_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"asl_ip\",\"type\":[\"string\",\"null\"]},{\"name\":\"access_time\",\"type\":[{\"type\":\"int\",\"logicalType\":\"date\"},\"null\"]},{\"name\":\"asl_url\",\"type\":[\"string\",\"null\"]},{\"name\":\"product_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"supplier_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"s_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"s_name\",\"type\":[\"string\",\"null\"]}]}");
        System.out.println(schema);
        GenericRecord record1 = new GenericData.Record(schema);

        record1.put("access_time", 123.45);
        System.out.println(record1);
        List<String> list = Lists.newArrayList(record1.toString());
        Dataset<Row> df = spark.sqlContext().read().json(jsc.parallelize(list, 2));
        writeHudi(df, SaveMode.Overwrite, false, "access_time", "access_time");
    }

    @Test
    public void test() throws IOException {
        StructField structField = new StructField("ad_shipping_free", DataTypes.createDecimalType(8, 0), true, Metadata.empty());
        StructField structField1 = new StructField("bbb", DataTypes.TimestampType, true, Metadata.empty());
        StructType structType = new StructType(new StructField[]{structField, structField1});
        Pair<String, String> pair = getRecordNameAndNamespace("advt_variation");
        Schema avro1 = AvroConversionUtils.convertStructTypeToAvroSchema(structType, pair.getLeft(), pair.getRight());
        System.out.println("avro => " + avro1);
        GenericRecord record1 = new GenericData.Record(avro1);
        //Schema schema = new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"test.test.aaa\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":0}");
        //Schema schema = new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"test.test.ad_shipping_free\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}");
        Schema schema =  SchemaConverters.toAvroType(structField.dataType(), false, "advt_variation_record.ad_shipping_free", "hoodie.advt_variation");
        System.out.println("schema => " + schema.toString());
        BigDecimal bigDecimalValue = new BigDecimal("9.12");
        Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
        DecimalType decimalType = (DecimalType) structField.dataType();
        GenericFixed fixed = decimalConversions.toFixed(bigDecimalValue, schema, LogicalTypes.decimal(decimalType.precision(), decimalType.scale()));
        record1.put("ad_shipping_free", fixed);
        record1.put("bbb", System.currentTimeMillis());
        System.out.println("record1 => " + record1);
        HoodieRecord hoodieRecord = convertRow2Record(structType, "ccc", record1, "test", "test");
        writeClient(avro1, "test", Lists.newArrayList(hoodieRecord));
    }

    @Test
    public void testGetUserConfigDecimals() {
        System.out.println(getUserConfigDecimals("a,10,2:b,5,2:c,6,1"));
    }

    @Test
    public void test0() {
        StructField structField = new StructField("aa", DataTypes.createDecimalType(), true, Metadata.empty());
        StructField structField1 = new StructField("bb", DataTypes.TimestampType, true, Metadata.empty());

        StructType structType = new StructType(new StructField[]{structField, structField1});
        List<Object> arrayList = new ArrayList<>();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        arrayList.add("222.22");
        arrayList.add(timestamp);
        Object[] objectArray = arrayList.toArray(new Object[0]);
        Row row = new GenericRowWithSchema(objectArray, structType);
        System.out.println(row.schema());
        System.out.println(row);
        System.out.println("size " + row.size());

        Schema avro = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"dyn_prod_cost_history_record\",\"namespace\":\"hoodie.dyn_prod_cost_history\",\"fields\":[{\"name\":\"_hoodie_is_deleted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"prod_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"so_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"dyn_prod_cost\",\"type\":[\"string\",\"null\"]},{\"name\":\"create_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"update_time\",\"type\":[\"string\",\"null\"]},{\"name\":\"dyn_prod_cost_bak\",\"type\":[\"string\",\"null\"]},{\"name\":\"operation\",\"type\":[\"string\",\"null\"]}]}");

        GenericRecord record = new GenericData.Record(avro);

        System.out.println(avro);

        record.put("id", "209079");
        record.put("prod_id", "209079");
        record.put("so_code", "209079");
        record.put("dyn_prod_cost", "209079");
        record.put("dyn_prod_cost_bak", "209079");

        System.out.println(record);

        HoodieKey hoodieKey = new HoodieKey("xxx", "");
        OverwriteWithLatestAvroPayload overwriteWithLatestAvroPayload = new OverwriteWithLatestAvroPayload(record, 1);
        HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, overwriteWithLatestAvroPayload);
        System.out.println(hoodieRecord);

        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"orders_hold_record\",\"namespace\":\"hoodie.orders_hold\",\"fields\":[{\"name\":\"_hoodie_is_deleted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"oh_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"orders_code\",\"type\":[\"string\",\"null\"]},{\"name\":\"oh_date\",\"type\":[\"string\",\"null\"]},{\"name\":\"oh_create_date\",\"type\":[\"string\",\"null\"]}]}");
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("_hoodie_is_deleted", true);
        record1.put("oh_id", 571500);
        record1.put("orders_code", "true");
        record1.put("oh_date", "AM2020082471617");
        record1.put("oh_create_date", "true");
        //record1.put("operation", "DELETE");

        HoodieKey hoodieKey1 = new HoodieKey("xxx", "");
        OverwriteWithLatestAvroPayload overwriteWithLatestAvroPayload1 = new OverwriteWithLatestAvroPayload(record1, 1);
        HoodieRecord hoodieRecord1 = new HoodieRecord(hoodieKey1, overwriteWithLatestAvroPayload1);
        System.out.println(hoodieRecord1);
    }

    @Test
    public void test03() {
        System.out.println(Utils.getRecordNameAndNamespace("test"));
    }

    @Test
    public void testGetParquetSchema() throws IOException {
        Path parquetFilePath = new Path("/Users/sflee/Downloads/f358bec6-64a4-4d90-b99f-4515e3cbead1-0_0-6558-6570_20200905212400.parquet");
        ParquetMetadata fileFooter =
                ParquetFileReader.readFooter(spark.sparkContext().hadoopConfiguration(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        System.out.println(fileFooter.getFileMetaData().getSchema());
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(spark.sparkContext().hadoopConfiguration());
        System.out.println(avroSchemaConverter.convert(fileFooter.getFileMetaData().getSchema()));
    }

    @Test
    public void test01() throws IOException {
        StructField structField = new StructField("aaa", DataTypes.createDecimalType(), true, Metadata.empty());
        StructField structField1 = new StructField("bbb", DataTypes.TimestampType, true, Metadata.empty());
        StructType structType = new StructType(new StructField[]{structField, structField1});
        Schema avro1 = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "test", "test");
        System.out.println("avro => " + avro1);

        GenericRecord record1 = new GenericData.Record(avro1);
        record1.put("aaa","111");
        //record1.put("bbb", "111111");
        ;
        //Dataset<Row> rows = AvroConversionUtils.createDataFrame(jsc.parallelize(Lists.newArrayList(record1)).rdd(), avro1.toString(), spark);
        Dataset<Row> rows = toDataset(record1);

        JavaRDD<GenericRecord> recordRDD = AvroConversionUtils.createRdd(rows, avro1, "test", "test").toJavaRDD();
        //JavaRDD<HoodieRecord> hoodieRecordJavaRDD = recordRDD.map(new MapFunction(new HashMap<>()));


        //Function1<Object, Object> convertor = AvroConversionHelper.createConverterToAvro(avro1, structType, "test", "test");
        //record1 = (GenericRecord) convertor.apply(record1);

        //HoodieRecord hoodieRecord = convertRow2Record(structType, "aaa", record1, "test", "test");
        //writeClient(avro1, "test", Lists.newArrayList(hoodieRecord));
        //writeClient(avro1, "test", hoodieRecordJavaRDD);
        //Schema schema = new Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"test.test.aaa\",\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":0}");
    }

    public static HoodieRecord convertRow2Record(StructType type, String recordKey, GenericRecord record, String structName, String namespace) {
        HoodieKey hoodieKey = new HoodieKey("aaa", "");

        OverwriteWithLatestAvroPayload overwriteWithLatestAvroPayload = new OverwriteWithLatestAvroPayload(record, 1L);
        HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, overwriteWithLatestAvroPayload);

        return hoodieRecord;
    }

    public static Dataset<Row> toDataset(GenericRecord genericRecord) {
        List<String> list = Lists.newArrayList(genericRecord.toString());
        Dataset<Row> df = spark.sqlContext().read().json(jsc.parallelize(list, 2));
        return df;
    }


    static class Name {
        int age;
        String name;

        public Name(String name, int age) {
            this.age = age;
            this.name = name;
        }

        @Override
        public String toString() {
            return "name = " + name + ", age = " + age;
        }
    }

    private static void writeHudi(Dataset<Row> df, SaveMode saveMode, boolean enableSyncDLA, String precombine, String recordKey) {
        String tableName = "test";
        String dbName = "test_dts_datatype";
        String dlaUsername = "xxx";
        String dlaPassword = "xxx";
        String dlaJdbcUrl = "xxx";
        String tableType = "COPY_ON_WRITE";
        String basePath = "/tmp/hudi_dts_data_test";
        if (enableSyncDLA) {
            df.write().format("org.apache.hudi").
                    options(getQuickstartWriteConfigs()).
                    option(PRECOMBINE_FIELD_OPT_KEY(), precombine).
                    option(RECORDKEY_FIELD_OPT_KEY(), recordKey).
                    option(PARTITIONPATH_FIELD_OPT_KEY(), "").
                    option(KEYGENERATOR_CLASS_OPT_KEY(), NonpartitionedKeyGenerator.class.getName()).
                    option("hoodie.embed.timeline.server", false).
                    option(TABLE_NAME, tableName).
                    option(TABLE_TYPE_OPT_KEY(), tableType).
                    option("hoodie.meta.sync.client.tool.class", "com.aliyun.dla.hudi.DLASyncTool").
                    option("hoodie.datasource.dla_sync.database", dbName).
                    option("hoodie.datasource.dla_sync.table", tableName).
                    option("hoodie.datasource.dla_sync.username", dlaUsername).
                    option("hoodie.datasource.dla_sync.password", dlaPassword).
                    option("hoodie.datasource.dla_sync.jdbcurl", dlaJdbcUrl).
                    option("hoodie.datasource.dla_sync.partition_fields", "").
                    option("hoodie.datasource.dla_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor").
                    mode(saveMode).
                    save(basePath);
        } else {
            df.write().format("org.apache.hudi").
                    options(getQuickstartWriteConfigs()).
                    option(PRECOMBINE_FIELD_OPT_KEY(), precombine).
                    option(RECORDKEY_FIELD_OPT_KEY(), recordKey).
                    option(PARTITIONPATH_FIELD_OPT_KEY(), "").
                    option(KEYGENERATOR_CLASS_OPT_KEY(), NonpartitionedKeyGenerator.class.getName()).
                    option("hoodie.embed.timeline.server", false).
                    option(TABLE_NAME, tableName).
                    option(TABLE_TYPE_OPT_KEY(), tableType).
                    mode(saveMode).
                    save(basePath);
        }
    }

    public static void writeClient(Schema schema, String tableName, List<HoodieRecord> lists) throws IOException {
        String basePath = "/tmp/hudi_dts_data_test";
        String tableType = "COPY_ON_WRITE";
        HoodieWriteConfig hoodieWriteConfig = new HoodieWriteConfig.Builder()
                .withSchema(schema.toString())
                .forTable(tableName)
                .withCompactionConfig(new HoodieCompactionConfig.Builder().withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(10).build())
                .withEmbeddedTimelineServerEnabled(false)
                .withParallelism(2, 2)
                .withPath(basePath)
                .build();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        init(jsc.hadoopConfiguration(), tableName, basePath, HoodieTableType.valueOf(tableType));
        HoodieWriteClient client = new HoodieWriteClient(jsc, hoodieWriteConfig);
        String time = client.startCommit();
        JavaRDD<HoodieRecord> rowJavaRDD = jsc.parallelize(lists, 1);
        client.upsert(rowJavaRDD, time);
    }

    public static void writeClient(Schema schema, String tableName, JavaRDD<HoodieRecord> recordJavaRDD) throws IOException {
        String basePath = "/tmp/hudi_dts_data_test";
        String tableType = "COPY_ON_WRITE";
        HoodieWriteConfig hoodieWriteConfig = new HoodieWriteConfig.Builder()
                .withSchema(schema.toString())
                .forTable(tableName)
                .withCompactionConfig(new HoodieCompactionConfig.Builder().withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(10).build())
                .withEmbeddedTimelineServerEnabled(false)
                .withParallelism(2, 2)
                .withPath(basePath)
                .build();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        init(jsc.hadoopConfiguration(), tableName, basePath, HoodieTableType.valueOf(tableType));
        HoodieWriteClient client = new HoodieWriteClient(jsc, hoodieWriteConfig);
        String time = client.startCommit();
        client.upsert(recordJavaRDD, time);
    }
}
