package com.aliyun.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkOSS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark oss test")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    //oss path format: oss://your_bucket_name/your/path
    val inputPath = args(0)
    val outputPath = args(1)
    val data = spark.read.format("csv").load(inputPath)
    data.show()
    data.write.csv(outputPath)
    spark.stop()
  }
}