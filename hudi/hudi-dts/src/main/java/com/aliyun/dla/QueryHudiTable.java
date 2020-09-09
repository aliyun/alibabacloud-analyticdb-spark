package com.aliyun.dla;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 用于通过Spark SQL查询Hudi数据集
 */
public class QueryHudiTable {
    private static final String basePath = "/tmp/hudi_dts_demo_new/new_dts_hudi_test/decimal_table/";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkHudiDtsDemo")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> tripsPointInTimeDF = spark.sqlContext().read().format("org.apache.hudi")/*.option("mergeSchema", "true")*/
                .load(basePath + "/*");
        tripsPointInTimeDF.createOrReplaceTempView("hudi_ro");

        spark.sql("select * from hudi_ro").printSchema();
        spark.sql("select * from hudi_ro").show();

    }
}
