package com.aliyun.spark.ganos.datasource

import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.geometry._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 从GeoMesa加载矢量数据
  */
object GeoMesa extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  val spark: SparkSession = {
    val session = SparkSession.builder
      .master("local[*]")
      .config(additionalConf)
      .getOrCreate()
    session
  }

  spark.withGanosGeometry

  //配置GeoMesa连接信息
  val dsParams: Map[String, String] = Map("hbase.zookeepers"->"localhost:2181","hbase.catalog"->"AIS")
  //val df=spark.read.ganos.geomesa(dsParams,"point")
  //df.show

  def additionalConf = new SparkConf(false)

}
