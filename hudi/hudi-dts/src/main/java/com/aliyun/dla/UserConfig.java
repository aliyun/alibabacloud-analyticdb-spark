package com.aliyun.dla;

/**
 * 用户相关配置类
 */
public class UserConfig {

    // dts相关配置
    public static String DTS_USERNAME = "dtsUsername";
    public static String DTS_PASSWORD = "dtsPassword";
    public static String OFFSET = "offset";
    public static String GROUPID = "groupId";
    public static String BOOTSTRAP_SERVER = "bootstrapServer";
    public static String MAX_OFFSET_PER_TRIGGER = "maxOffsetsPerTrigger";
    public static String SUBSCRIBE_TOPIC = "subscribeTopic";
    public static String FETCH_MESSAGE_MAX_BYTES = "fetch.message.max.bytes";

    /**
     * NOTE: 在增量同步时如果进行类型转化，那么在存量同步时也需要进行对应的类型转化，否则会有类型不匹配问题
     */
    // 将所有类型都转化为String类型
    public static String CONVERT_ALL_TYPES_TO_STRING = "convert.all.types.to.string";
    // 将Decimal类型转化为String类型
    public static String CONVERT_DECIMAL_TO_STRING = "convert.decimal.to.string";
    // 设置Decimal的Precision
    public static String DECIMAL_COLUMNS_DEFINITION = "decimal.columns.definition";

    // hudi相关配置
    public static String BATH_PATH = "basePath";
    public static String HOODIE_COMPACT_INLINE = "hoodie.compact.inline";
    public static String HOODIE_COMPACT_INLINE_MAX_DELTA_COMMITS = "hoodie.compact.inline.max.delta.commits";
    public static String HOODIE_TABLE_TYPE = "hoodie.table.type";
    public static String HOODIE_ENABLE_TIMELINE_SERVER = "hoodie.enable.timeline.server";
    public static String HOODIE_INSERT_SHUFFLE_PARALLELISM = "hoodie.insert.shuffle.parallelism";
    public static String HOODIE_UPSERT_SHUFFLE_PARALLELISM = "hoodie.upsert.shuffle.parallelism";

    // dla相关配置
    public static String ENABLE_SYNC_TO_DLA = "enable.sync.to.dla";
    public static String DLA_USERNAME = "dla.username";
    public static String DLA_PASSWORD = "dla.password";
    public static String DLA_JDBC_URL = "dla.jdbc.url";
    public static String DLA_DATABASE = "dla.database";
}
