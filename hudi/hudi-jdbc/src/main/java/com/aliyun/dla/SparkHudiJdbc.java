package com.aliyun.dla;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class SparkHudiJdbc {
    private static final Logger LOG = LogManager.getLogger(SparkHudiJdbc.class);

    /**
     * Jdbc Config
     */
    private static String URL = "jdbc.url";
    private static String DB_NAME = "jdbc.dbName";
    private static String TABLE_NAME = "jdbc.tableName";
    private static String USERNAME = "jdbc.username";
    private static String PASSWORD = "jdbc.password";
    private static String FETCH_SIZE = "jdbc.fetchSize";
    private static String TOTAL_COUNT = "jdbc.totalCount";
    private static String START_OFFSET = "jdbc.startOffset";
    private static String SPLIT_FIELD = "jdbc.splitField";
    private static String BATCH_SIZE = "jdbc.batchSize";

    /**
     * Hudi Config
     */
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

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkHudiJdbcDemo")
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

        String dbName = getConfig(properties, DB_NAME, "");
        String tableName = getConfig(properties, TABLE_NAME, "");
        String url = getConfig(properties, URL, "");
        if (StringUtils.isEmpty(dbName)
                || StringUtils.isEmpty(tableName)
                || StringUtils.isEmpty(url)) {
            LOG.error("dbName or tableName or url is empty.");
            throw new RuntimeException("dbName or tableName or url is empty.");
        }

        int batchSize = Integer.parseInt(getConfig(properties, BATCH_SIZE, "100000"));
        long startOffset = Long.parseLong(getConfig(properties, START_OFFSET, "0"));
        long totalCount = Long.parseLong(getConfig(properties, TOTAL_COUNT, "10000000"));
        long endOffset = startOffset + batchSize;

        String splitField = getConfig(properties, SPLIT_FIELD, "");
        String sqlFormat;
        Dataset<Row> dataset;
        if (StringUtils.isEmpty(splitField)) {
            sqlFormat = "(select * from %s.%s) temp";
            String sql = String.format(sqlFormat, dbName, tableName);
            dataset = spark.read().format("jdbc")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("url", "jdbc:mysql://127.0.0.1:3306/my_test")
                    .option("dbtable", sql)
                    .option("user", "leesf")
                    .option("password", "123456")
                    .option("fetchsize", "1000")
                    .load();
        } else {
            sqlFormat = "(select * from %s.%s where %s <= %s and %s < %s) temp";
        }


        /*while (endOffset <= totalCount) {

            String sql = String.format(sqlFormat, dbName, tableName, startOffset, endOffset);

            System.out.println(sql);

            Dataset<Row> dataset = spark.read().format("jdbc")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("url", "jdbc:mysql://127.0.0.1:3306/my_test")
                    .option("dbtable", sql)
                    .option("user", "leesf")
                    .option("password", "123456")
                    .option("fetchsize", "1000")
                    .load();

            dataset.show();
            start = end;
            end = end + batchSize;
            System.out.println("start = " + start + ", end = " + end);
            Thread.sleep(1000);
        }*/
    }

    public static String getConfig(Properties properties, String key, String defaultValue) {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        }
        return defaultValue;
    }

    public static void mysql() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/my_test?useSSL=true","leesf","123456");
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from student");
        int count = resultSet.getMetaData().getColumnCount();
        for (int i = 1; i <= count; i++) {
            while (resultSet.next()) {
                System.out.print(resultSet.getString(i) + "\t");
            }
        }

    }
}
