package com.aliyun.spark

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Spark HIVE TEST")
      .enableHiveSupport()
      .getOrCreate()

    val welcome = "hello, dla-spark"

    //Hive表名
    val tableName = args(0)

    import sparkSession.implicits._
    //将只有一行 一列数据的DataFrame: df 存入到Hive, 表名为用户传进来的tableName, 列名为welcome_col
    val df = Seq(welcome).toDF("welcome_col")
    df.write.format("hive").mode("overwrite").saveAsTable(tableName)

    //从Hive中读取表 tableName
    val dfFromHive = sparkSession.sql(
      s"""
        |select * from $tableName
        |""".stripMargin)
    dfFromHive.show(10)
  }
}
