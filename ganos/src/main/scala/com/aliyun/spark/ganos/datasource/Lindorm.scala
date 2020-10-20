package com.aliyun.spark.ganos.datasource

import java.net.URI

import geotrellis.raster.Tile
import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.lindorm._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 从Lindorm(HBase)加载栅格数据
  */
object Lindorm extends App{

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

  import spark.implicits._

  //Lindorm连接地址，格式如：ld-[Lindorm ID]-proxy-hbaseue-pub.hbaseue.rds.aliyuncs.com:30020
  val inputUri = "[Lindorm连接地址]?attributes=[Attribute表名称]"

  def additionalConf = new SparkConf(false)

  //读取图层catalog信息
  val cat=spark.read.ganos.lindormRasterCatalog(URI.create(inputUri))
  cat.show

  //加载图层
  val layer = cat.where($"layer.id.zoom" === "2").select(raster_layer).collect
  val lots = layer.map(spark.read.ganos.lindormRaster.loadLayer).map(_.toDF).reduce(_ union _)
  lots.show

  //抵用st_rgb_composite生成RGB 瓦片
  val expr = lots.select(st_rgb_composite($"tile_3", $"tile_2", $"tile_1"))
  expr.show()
  val nat_color = expr.first()
  nat_color.getAs[Tile](0).renderPng().write("rgb.png")

}
