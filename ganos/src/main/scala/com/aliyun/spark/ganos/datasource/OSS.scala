package com.aliyun.spark.ganos.datasource

import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.oss._
import com.aliyun.ganos.dla.geometry._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 从OSS加载栅格数据
  */
object OSS extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  val spark: SparkSession = {
    val session = SparkSession.builder
      .withKryoSerialization
      .config(additionalConf)
      .getOrCreate()
    session
  }

  spark.withGanosGeometry
  spark.withGanosRaster

  val uri = new java.net.URI("OSS地址")
  val options = Map(
    "crs"->"EPSG:4326",
    "endpoint" -> "OSS endpoint地址",
    "accessKeyId" -> "客户accessKeyId",
    "accessKeySecret" -> "客户accessKeySecret")
  val rf = spark.read.ganos.oss(options).loadLayer(uri)
  rf.show
  def additionalConf = new SparkConf(false)

}
