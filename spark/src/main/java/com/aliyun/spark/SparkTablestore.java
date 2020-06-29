package com.aliyun.spark;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.aliyun.openservices.tablestore.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class SparkTablestore {
    /**
     * With range query to fetch all the rows in your table
     *
     * @return table store query handle
     */
    private static RangeRowQueryCriteria fetchCriteria() {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria("YourTableName");
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        lower.add(new PrimaryKeyColumn("YourPkeyName", PrimaryKeyValue.INF_MIN));
        upper.add(new PrimaryKeyColumn("YourPkeyName", PrimaryKeyValue.INF_MAX));
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    public static void main(String[] args) {
        // init spark context and config
        SparkConf sparkConf = new SparkConf().setAppName("RowCounter");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration hadoopConf = new Configuration();

        /*
           init Tablestore environment
           Highlight: the third parameter of Credential can be null for MAIN USER
                new Credential("YourAccessKeyId", "YourAccessKeySecret", null)
         */
        TableStoreInputFormat.setCredential(
                hadoopConf,
                new Credential("YourAccessKeyId", "YourAccessKeySecret", "YourStsToken, can be null"));
        TableStoreInputFormat.setEndpoint(
                hadoopConf,
                new Endpoint("https://YourInstance.Region.ots.aliyuncs.com/"));
        TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria());

        try {
            // read data from Tablestore and output the rows count
            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
                    hadoopConf,
                    TableStoreInputFormat.class,
                    PrimaryKeyWritable.class,
                    RowWritable.class);
            System.out.println(
                    new Formatter().format("TOTAL ROWS: %d", rdd.count()).toString());
        } finally {
            sc.close();
        }
    }

}
