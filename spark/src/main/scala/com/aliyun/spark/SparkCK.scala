package com.aliyun.spark

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * create ck table, read csv value and write to ck table, and the read value from ck.
 */
object SparkCK {

  def createCKTable(table: String, url: String, ckProperties: Properties): Unit ={
    Class.forName(ckProperties.getProperty("driver"))
    var conn : Connection = null
    try {
      conn = DriverManager.getConnection(url, ckProperties.getProperty("user"), ckProperties.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        s"""
           |create table if not exists default.${table}  on cluster default(
           |    `name` String,
           |    `age`  Int32)
           |ENGINE = MergeTree() ORDER BY `name` SETTINGS index_granularity = 8192;
           |""".stripMargin
      stmt.executeQuery(sql)
    } finally {
      if(conn != null)
        conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 4){
      System.err.println(
        """
          |arg0: clickhouse jdbc url
          |args1: clickhouse user name
          |args2:  password
          |args3 csv path in oss
          |""".stripMargin)

      System.exit(1)
    }

    val ckProperties = new Properties()
    val url = args(0)
    val user = args(1)
    val password = args(2)
    val csvPath = args(3)
    ckProperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    ckProperties.put("user", user)
    ckProperties.put("password", password)
    ckProperties.put("batchsize","100000")
    ckProperties.put("socket_timeout","300000")
    ckProperties.put("numPartitions","8")
    ckProperties.put("rewriteBatchedStatements","true")

    val table = "ck_test"
    //使用jdbc建表
    createCKTable(table, url, ckProperties)
    val spark = SparkSession.builder().getOrCreate()
    //将csv的数据写入ck
    /**
     * 将下列数据保存到oss
     * name,age
     * fox,18
     * tiger,20
     * alice,36
     */
    val csvDF = spark.read.option("header","true").csv(csvPath).toDF("name", "age")
    csvDF.printSchema()
    csvDF.write.mode(SaveMode.Append).option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000).jdbc(url, table, ckProperties)

    //读ck表数据
    val ckDF = spark.read.jdbc(url, table, ckProperties)
    ckDF.show()
  }

}
