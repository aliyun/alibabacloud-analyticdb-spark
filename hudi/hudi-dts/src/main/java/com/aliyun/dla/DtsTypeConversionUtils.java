package com.aliyun.dla;

import com.alibaba.dts.formats.avro.Decimal;
import com.alibaba.dts.formats.avro.Field;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import recordprocessor.FieldValue;
import recordprocessor.mysql.MysqlFieldConverter;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static com.aliyun.dla.Utils.getRecordNameAndNamespace;

/**
 * DTS类型转化类
 */
public class DtsTypeConversionUtils {
    private static final Logger LOG = LogManager.getLogger(DtsTypeConversionUtils.class);

    public static final int DEFAULT_PRECISION = 10;
    public static final int DEFAULT_SCALE = 0;
    /**
     * 将DTS值转化为对应数据类型
     * @param tableName
     * @param field
     * @param dtsValue
     * @return
     */
    public static Object convertDtsValueToDataType(StructField structField, String tableName, Field field, Object dtsValue, boolean convertDecimalToString) {
        int dtsTypeId = field.getDataTypeNumber();
        if (dtsValue == null) {
            LOG.warn("dts value is null, dtsTypeId = " + dtsTypeId);
            return null;
        }
        LOG.debug("name = " + field.getName() + ", dtsTypeId = " + dtsTypeId + ", o.class name = " + dtsValue.getClass().getName() + ", toString = " + dtsValue.toString());
        switch (dtsTypeId) {
            case 0 : // DECIMAL
            case 246 : { // DECIMAL_NEW
                com.alibaba.dts.formats.avro.Decimal de = (com.alibaba.dts.formats.avro.Decimal) dtsValue;
                if (convertDecimalToString) {
                    return de.getValue();
                } else {
                    BigDecimal bigDecimalValue = new BigDecimal(de.getValue());
                    Pair<String, String> recordAndNamespacePair = getRecordNameAndNamespace(tableName);
                    Schema schema = SchemaConverters.toAvroType(structField.dataType(), false, recordAndNamespacePair.getLeft() + "." + field.getName(), recordAndNamespacePair.getRight());
                    Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();
                    DecimalType sparkDecimal = (DecimalType) structField.dataType();
                    int precision = sparkDecimal.precision();
                    int scale = sparkDecimal.scale();
                    GenericFixed fixed = decimalConversions.toFixed(bigDecimalValue, schema, LogicalTypes.decimal(precision, scale));
                    return fixed;
                }
            }
            case 1 : // INT8
            case 2 : // INT16
            case 3 : // INT32
            case 9 : { // INT24
                com.alibaba.dts.formats.avro.Integer integer = (com.alibaba.dts.formats.avro.Integer) dtsValue;
                return Integer.parseInt(integer.getValue());
            }
            case 4 : { // FLOAT
                com.alibaba.dts.formats.avro.Float aFloat = (com.alibaba.dts.formats.avro.Float) dtsValue;
                return Float.parseFloat(aFloat.getValue().toString());
            }
            case 5 : { // DOUBLE
                com.alibaba.dts.formats.avro.Float f = (com.alibaba.dts.formats.avro.Float) dtsValue;
                return f.getValue();
            }
            case 6 : { // NULL
                MysqlFieldConverter mysqlFieldConverter = new MysqlFieldConverter();
                FieldValue value = mysqlFieldConverter.convert(field, dtsValue);
                return new String(value.getValue());
            }
            case 7 : // TIMESTAMP
            case 17 : { // TIMESTAMP_NEW
                com.alibaba.dts.formats.avro.Timestamp t = (com.alibaba.dts.formats.avro.Timestamp) dtsValue;
                Timestamp timestamp = new Timestamp(t.getTimestamp());
                return timestamp.getTime() * 1000;
            }
            case 8 : { // Type.INT64
                com.alibaba.dts.formats.avro.Integer integer = (com.alibaba.dts.formats.avro.Integer) dtsValue;
                return Long.parseLong(integer.getValue());
            }
            case 10 : // DATE
            case 14 : { // DATE_NEW
                com.alibaba.dts.formats.avro.DateTime dataTime = (com.alibaba.dts.formats.avro.DateTime) dtsValue;
                Date date = new Date(new Timestamp(
                        dataTime.getYear() == null ? 0 : dataTime.getYear() - 1900,
                        dataTime.getMonth() == null ? 0 : dataTime.getMonth() - 1,
                        dataTime.getDay() == null ? 0 : dataTime.getDay(),
                        dataTime.getHour() == null ? 00 : dataTime.getHour(),
                        dataTime.getMinute() == null ? 00 : dataTime.getMinute(),
                        dataTime.getSecond() == null ? 00 : dataTime.getSecond(),
                        dataTime.getMillis() == null ? 00 : dataTime.getMillis()).getTime());
                return new Long(date.toLocalDate().toEpochDay()).intValue();
            }
            case 11 : // TIME
            case 19 : { // TIME_NEW
                com.alibaba.dts.formats.avro.DateTime dataTime = (com.alibaba.dts.formats.avro.DateTime) dtsValue;
                return new Timestamp(
                        dataTime.getYear() == null ? 0 : dataTime.getYear() - 1900,
                        dataTime.getMonth() == null ? 0 : dataTime.getMonth() - 1,
                        dataTime.getDay() == null ? 0 : dataTime.getDay(),
                        dataTime.getHour() == null ? 00 : dataTime.getHour(),
                        dataTime.getMinute() == null ? 00 : dataTime.getMinute(),
                        dataTime.getSecond() == null ? 00 : dataTime.getSecond(),
                        dataTime.getMillis() == null ? 00 : dataTime.getMillis()).getTime() * 1000;
            }
            case 12 : // DATETIME
            case 18 : { // DATETIME_NEW
                com.alibaba.dts.formats.avro.DateTime dataTime = (com.alibaba.dts.formats.avro.DateTime) dtsValue;
                return new Timestamp(
                        dataTime.getYear() == null ? 0 : dataTime.getYear() - 1900,
                        dataTime.getMonth() == null ? 0 : dataTime.getMonth() - 1,
                        dataTime.getDay() == null ? 0 : dataTime.getDay(),
                        dataTime.getHour() == null ? 00 : dataTime.getHour(),
                        dataTime.getMinute() == null ? 00 : dataTime.getMinute(),
                        dataTime.getSecond() == null ? 00 : dataTime.getSecond(),
                        dataTime.getMillis() == null ? 00 : dataTime.getMillis()).getTime() * 1000;
            }
            case 13: { // Type.YEAR
                com.alibaba.dts.formats.avro.DateTime dateTime = (com.alibaba.dts.formats.avro.DateTime) dtsValue;
                return Integer.toString(dateTime.getYear());
            }
            case 15: { // TYPE.VARCHAR
                com.alibaba.dts.formats.avro.Character c = (com.alibaba.dts.formats.avro.Character) dtsValue;
                return new String(c.getValue().array(), StandardCharsets.UTF_8);
            }
            case 16 : { // BIT
                com.alibaba.dts.formats.avro.Integer integer = (com.alibaba.dts.formats.avro.Integer) dtsValue;
                return integer.getValue();
            }
            case 245 : // TYPE.JSON
            case 247 : // TYPE.ENUM
            case 248 : { // TYPE.SET
                com.alibaba.dts.formats.avro.TextObject textObject = (com.alibaba.dts.formats.avro.TextObject) dtsValue;
                return textObject.getValue();
            }
            case 249 : // TINY_BLOB
            case 250 : // TYPE.MEDIUM_BLOB
            case 251 : // LONG_BLOB
            case 252 : { // BLOB
                com.alibaba.dts.formats.avro.BinaryObject binaryObject = (com.alibaba.dts.formats.avro.BinaryObject) dtsValue;
                return new String(binaryObject.getValue().array(), StandardCharsets.UTF_8);
            }
            case 253 : // VAR_STRING
            case 254 : { // STRING
                com.alibaba.dts.formats.avro.Character c1 = (com.alibaba.dts.formats.avro.Character) dtsValue;
                return new String(c1.getValue().array(), StandardCharsets.UTF_8);
            }
            case 255 : { // GEOMETRY
                com.alibaba.dts.formats.avro.BinaryGeometry geometry = (com.alibaba.dts.formats.avro.BinaryGeometry) dtsValue;
                return new String(geometry.getValue().array(), StandardCharsets.UTF_8);
            }
            default:
                throw new RuntimeException("not supported type = " + dtsTypeId);
        }
    }

    public static StructField convertStructTypeToStringType(StructField structField) {
        StructField field = new StructField(structField.name(), DataTypes.StringType, true, Metadata.empty());
        return field;
    }

    public static StructField buildStructField(Field field, String fieldName, Object value, boolean convertDecimalToString, Map<String, Pair<Integer, Integer>> decimalColumns) {
        int dtsTypeId = field.getDataTypeNumber();
        // https://help.aliyun.com/document_detail/121239.html?spm=5176.2020520151.0.dexternal.65b575b0jDLl2P
        switch (dtsTypeId) {
            case 0 : // Type.DECIMAL
            case 246 : { // Type.DECIMAL_NEW
                if (convertDecimalToString) {
                    return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty());
                } else {
                    /**
                     * NOTE: 测试发现dts对于源库表(new_dts_hudi_test:advt_variation)中类型为decimal(8,2)返回的precision和scale与数据强相关，
                     * 获取不到原来的8, 2。如定义为decimal(8, 2),但实际数据如下
                     * 1.11, 此时precision = 3, scale = 2;
                     * 11.11, 此时precision = 4, scale = 2;
                     * 11, 此时precision = 2, scale = 0;
                     * 而不同scale是无法兼容的，会报错; 如果真实数据都是1.11, 2.12, 20.22这种保留小数点两位的
                     * 则可固定scale取值，而precision可取值稍大点.
                     * 另外还可支持用户自定义Decimal列类型
                     */
                    int precision = DEFAULT_PRECISION;
                    int scale = DEFAULT_SCALE;
                    if (decimalColumns.containsKey(fieldName)) {
                        Pair<Integer, Integer> pair = decimalColumns.get(fieldName);
                        precision = pair.getLeft();
                        scale = pair.getRight();
                    } else {
                        if (null != value) {
                            Decimal decimal = (Decimal) value;
                            precision = tryGetSafePrecision(decimal.getPrecision());
                            scale = decimal.getScale();
                        }
                    }
                    return new StructField(fieldName, DataTypes.createDecimalType(precision, scale), true, Metadata.empty());
                }
            }
            case 1 : // TYPE.INT8
            case 2 : // TYPE.INT16
            case 3 : // TYPE.INT32
            case 9 : // TYPE.INT24
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty());
            case 4 : // TYPE.FLOAT
                return new StructField(fieldName, DataTypes.FloatType, true, Metadata.empty());
            case 5 : // TYPE.DOUBLE
                return new StructField(fieldName, DataTypes.DoubleType, true, Metadata.empty());
            case 6 : // TYPE.NULL
                return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty());
            case 7 : // TYPE.TIMESTAMP
            case 17 : // TYPE.TIMESTAMP_NEW
                return new StructField(fieldName, DataTypes.TimestampType, true, Metadata.empty());
            case 8 : // TYPE.INT64
                return new StructField(fieldName, DataTypes.LongType, true, Metadata.empty());
            case 10 : // TYPE.DATE
            case 14 : // TYPE.DATE_NEW
                return new StructField(fieldName, DataTypes.DateType, true, Metadata.empty());
            case 11 : // TYPE.TIME
            case 19 : // TYPE.TIME_NEW
                return new StructField(fieldName, DataTypes.TimestampType, true, Metadata.empty());
            case 12 : // TYPE.DATETIME
            case 18 : // TYPE.DATETIME_NEW
                return new StructField(fieldName, DataTypes.TimestampType, true, Metadata.empty());
            case 13 : // TYPE.YEAR
                return new StructField(fieldName, DataTypes.DateType, true, Metadata.empty());
            case 15 : // TYPE.VARCHAR
                return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty());
            case 16 : // TYPE.BIT
                return new StructField(fieldName, DataTypes.IntegerType, true, Metadata.empty());
            case 245 : // TYPE.JSON
            case 247 : // TYPE.ENUM
            case 248 : // TYPE.SET
            case 249 : // TYPE.TINY_BLOB
            case 250 : // TYPE.MEDIUM_BLOB
            case 251 : // TYPE.LONG_BLOB
            case 252 : // TYPE.BLOB
            case 253 : // Type.VAR_STRING;
            case 254 : // Type.STRING;
            case 255 : // TYPE_GEOMETRY;
                return new StructField(fieldName, DataTypes.StringType, true, Metadata.empty());
            default:
                throw new RuntimeException("not supported type = " + dtsTypeId);
        }
    }

    /**
     * 尽可能获取一个安全的precision
     * 如果precision为1，则返回10
     * 如果为11，则返回20
     * 如果为10，则返回20
     * @param precision
     * @return
     */
    public static int tryGetSafePrecision(int precision) {
        return ((precision / 10) + 1) * 10;
    }
}
