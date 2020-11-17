package com.aliyun.spark.ganos


import com.aliyun.ganos.dla.geometry._
import com.aliyun.ganos.dla.oss._
import com.aliyun.ganos.dla.raster._
import geotrellis.layer._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.TileLayerRDD
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.hbase.HBaseLayerWriter
import geotrellis.store._
import geotrellis.store.hbase.{HBaseAttributeStore, HBaseInstance}
import geotrellis.store.index.ZCurveKeyIndexMethod
import com.aliyun.ganos.dla._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 加载OSS中的GeoTiff文件为RDD，并写入Lindorm(HBase)
  */
object OSS2HBase extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  implicit val spark: SparkSession = {
    val session = SparkSession.builder
      .master("local[*]")
      .withKryoSerialization
      .config(additionalConf)
      .getOrCreate()
    session
  }

  import spark.implicits._

  spark.withGanosGeometry
  spark.withGanosRaster

  //加载OSS文件
  val uri1 = new java.net.URI("OSS地址/srtm_60_05.tif")
  val uri2 = new java.net.URI("OSS地址/srtm_61_05.tif")
  val options = Map(
    "crs"->"EPSG:4326",
    "endpoint" -> "OSS地址",
    "accessKeyId" -> "客户accessKeyId",
    "accessKeySecret" -> "客户accessKeySecret")
  val rf1 = spark.read.ganos.oss(options).loadLayer(uri1)
  val rf2 = spark.read.ganos.oss(options).loadLayer(uri2)
  val df = rf1 union rf2
  val rdd:(Int,TileLayerRDD[SpatialKey]) = df.asGanosRasterLayer.toTileLayoutRDD(st_geometry($"extent"), $"tile")
  val tiles:TileLayerRDD[SpatialKey]= rdd._2

  //定义Lindorm(HBase)数据源
  val layerName="oss"
  val instance = HBaseInstance(Seq("Lindorm连接地址"), "localhost")
  val attributeStore = HBaseAttributeStore(instance, "SRTM")
  val writer = HBaseLayerWriter(attributeStore, "SRTM_TILE")

  val layoutScheme=ZoomedLayoutScheme(WebMercator, tileSize = 256)

  //创建金字塔并写入Lindorm
  Pyramid.upLevels(tiles, layoutScheme, rdd._1, Bilinear) { (rdd, z) =>
    val layerId = LayerId(layerName, z)
    writer.write(layerId, rdd, ZCurveKeyIndexMethod)
  }

  def additionalConf = new SparkConf(false)
}
