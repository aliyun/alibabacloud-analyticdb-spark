package com.aliyun.spark

import org.apache.spark.sql.SparkSession

object SparkHDFS {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Spark HDFS TEST")
      .getOrCreate()

    val welcome = "hello, dla-spark"

    //hdfs目录用于存放内容
    val hdfsPath = args(0)
    //将welcome字符串存入指定的hdfs目录
    sparkSession.sparkContext.parallelize(Seq(welcome)).saveAsTextFile(hdfsPath)
    //从指定的hdfs目录中读取内容，并打印
    println("----------------------------------------------------------")
    sparkSession.sparkContext.textFile(hdfsPath).collect.foreach(println)
    println("-----------------------------------------------------------")
  }
}
