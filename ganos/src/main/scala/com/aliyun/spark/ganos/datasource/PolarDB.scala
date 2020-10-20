package com.aliyun.spark.ganos.datasource

import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.geometry._
import com.aliyun.ganos.dla.polardb._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 从PolarDB加载栅格数据
  */
object PolarDB extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  val spark: SparkSession = {
    val session = SparkSession.builder
      .master("local[*]")
      .withKryoSerialization
      .config(additionalConf)
      .getOrCreate()
    session
  }
  spark.withGanosRaster
  spark.withGanosGeometry


  val options = Map(
    "url" -> "PolarDB数据库URL",
    "user" -> "用户名",
    "password" -> "密码",
    "dbtable" -> "数据库名称",
    "numPartitions" -> "分区数")

  //读取图层catalog信息
  val cat=spark.read.ganos.polardbRasterCatalog(options)
  cat.show

  //根据ID读取图层
  val image=spark.read.ganos.polardbRasterImage(options).load(3, 4) //读取id=1，2，3的三幅图层的第六层金字塔
  image.show

  def additionalConf = new SparkConf(false)

}
