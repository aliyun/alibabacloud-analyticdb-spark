package com.aliyun.dla;

import com.alibaba.dts.formats.avro.Record;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hudi.AvroConversionUtils$;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import py4j.StringUtil;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.aliyun.dla.Constants.DBNAME;
import static com.aliyun.dla.Constants.META_LENGTH;
import static com.aliyun.dla.Constants.OPERATION;
import static com.aliyun.dla.Constants.SOURCE_TIMESTAMP;
import static com.aliyun.dla.Constants.SOURCE_TIMESTAMP_INDEX;
import static com.aliyun.dla.Constants.TABLENAME;
import static com.aliyun.dla.Constants.TBL_PKS;
import static com.aliyun.dla.Constants.TBL_PKS_INDEX;
import static com.aliyun.dla.Constants._HOODIE_IS_DELETED;
import static com.aliyun.dla.Constants._HOODIE_IS_DELETED_INDEX;

/**
 * 工具类
 */
public class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    public static Pair<String, String> getDBAndTablePair(Record record) {
        String objectName = record.getObjectName();
        String[] dbInfo = common.Util.uncompressionObjectName(objectName);
        List<String> dbTableNames = getDBTableName(dbInfo);

        String dbName = dbTableNames.get(0);
        String tableName = dbTableNames.get(1);

        return ImmutablePair.of(dbName, tableName);
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

    /**
     * 获取元数据的StructField列表
     * @return
     */
    public static List<StructField> getMetaStructFields() {
        List<StructField> structFields = new ArrayList<>();

        structFields.add(new StructField(SOURCE_TIMESTAMP, DataTypes.LongType, true, Metadata.empty()));
        structFields.add(new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()));
        structFields.add(new StructField(DBNAME, DataTypes.StringType, true, Metadata.empty()));
        structFields.add(new StructField(TABLENAME, DataTypes.StringType, true, Metadata.empty()));
        structFields.add(new StructField(TBL_PKS, DataTypes.StringType, true, Metadata.empty()));
        structFields.add(new StructField(_HOODIE_IS_DELETED, DataTypes.BooleanType, true, Metadata.empty()));

        return structFields;
    }


    /**
     * 获取HoodieRecord Key(主键)
     * @param row
     * @param originSchema
     * @return
     */
    public static String getHoodieRecordKey(Row row, Schema originSchema) {
        String pks = row.getString(TBL_PKS_INDEX);
        String[] keys = pks.split(",");
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            Schema.Field field  = originSchema.getField(key);
            sb.append(row.get(field.pos() + (META_LENGTH - 1))).append(",");
        }
        sb.deleteCharAt(sb.toString().length() - 1);
        return sb.toString();
    }

    public static Pair<String, String> getRecordNameAndNamespace(String tableName) {
        Tuple2<String, String> tuple = AvroConversionUtils$.MODULE$.getAvroRecordNameAndNamespace(tableName);
        return new ImmutablePair<>(tuple._1, tuple._2);
    }

    /**
     * 获取指定配置
     * @param properties
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getConfig(Properties properties, String key, String defaultValue) {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        }
        return defaultValue;
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

    /**
     * 将Row转化为HoodieRecord
     * @param row
     * @param originSchema
     * @return
     */
    public static HoodieRecord convertRow2Record(Row row, Schema originSchema) {
        long ts = Long.parseLong(row.get(SOURCE_TIMESTAMP_INDEX).toString());
        // 获取主键值
        String hoodieRecordKey = getHoodieRecordKey(row, originSchema);

        // 源库schema
        List<Schema.Field> fields = originSchema.getFields();
        GenericRecord record = new GenericData.Record(originSchema);
        record.put(_HOODIE_IS_DELETED, row.getBoolean(_HOODIE_IS_DELETED_INDEX));
        for (Schema.Field field : fields) {
            String name = field.name();
            if (_HOODIE_IS_DELETED.equals(name)) {
                continue;
            }
            if (row.get(field.pos() + META_LENGTH -1) == null) {
                LOG.info("name = " + name + " is null, skip");
                continue;
            }
            record.put(name, row.get(field.pos() + META_LENGTH - 1));
        }

        HoodieKey hoodieKey = new HoodieKey(hoodieRecordKey, "");
        LOG.info("record => " + record + ", originSchema => " + originSchema.toString());
        OverwriteWithLatestAvroPayload overwriteWithLatestAvroPayload = new OverwriteWithLatestAvroPayload(record, ts);
        HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, overwriteWithLatestAvroPayload);

        return hoodieRecord;
    }

    /**
     * 定义按照如下格式，即 表名:列名,precision,scale;列名1,precision,scale#表名1:列名1,precision,scale
     * t1:a,10,2;b,8,2#t2:a1,9,2;b1,5,2#...
     * @param decimals
     * @return
     */
    public static Map<String, Map<String, Pair<Integer, Integer>>> getUserConfigDecimals(String decimals) {
        Map<String, Map<String, Pair<Integer, Integer>>> map = new HashMap<>();
        if (StringUtils.isEmpty(decimals)) {
            return map;
        }
        String[] tableColumns = decimals.split("#");
        for (String tableColumn : tableColumns) {
            if (StringUtils.isNotEmpty(tableColumn)) {
                String[] tableAndColumns = tableColumn.split(":");
                String tableName = tableAndColumns[0];
                if (!map.containsKey(tableName)) {
                    map.put(tableName, new HashMap<>());
                }
                Map<String, Pair<Integer, Integer>> columnMap = map.get(tableName);
                String[] columns = tableAndColumns[1].split(";");
                for (String column : columns) {
                    String[] columnInfo = column.split(",");
                    columnMap.put(columnInfo[0], new ImmutablePair<>(Integer.parseInt(columnInfo[1]), Integer.parseInt(columnInfo[2])));
                }
            }
        }

        return map;
    }

}
