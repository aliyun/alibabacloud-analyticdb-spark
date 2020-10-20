package com.aliyun.spark.ganos.datasource

import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.raster.geotiff._
import org.apache.spark.sql.SparkSession

/**
  * 从本地读取GeoTiff
  */
object GeoTiff extends App{

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster

  val df1 = spark.read.ganos.geotiff.load(getClass.getResource("/LC08_L1TP_121035_20190702_20190706_01_T1.TIF").getPath).asLayer
  df1.show

}
