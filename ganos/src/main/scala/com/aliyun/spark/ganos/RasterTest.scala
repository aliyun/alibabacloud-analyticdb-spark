package com.aliyun.spark.ganos

import com.aliyun.spark.ganos.LocalAlgorithm.{bandNumbers, getClass, spark, st_}
import org.apache.spark.sql.SparkSession
import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object RasterTest extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster

  import spark.implicits._

  val df1 = spark.read.ganosRaster.withBandIndexes(0).withTileDimensions(512,512).load("/Users/phil/Data/GeoTIFF/demo/3420C_2010_327_RGB_LATLNG.tif")
  df1.select(
    st_extent(st_geometry($"proj_raster")).alias("extent"),
    st_tile($"proj_raster").alias("tile")
  ).show
}
