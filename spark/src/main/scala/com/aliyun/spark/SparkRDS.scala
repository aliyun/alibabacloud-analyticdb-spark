package com.aliyun.spark

import org.apache.spark.sql.SparkSession

object SparkRDS {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("rds test")
      .getOrCreate()

    val url = args(0)
    val dbtable = args(1)
    val user = args(2)
    val password = args(3)

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .load()

    jdbcDF.show()
  }

}
