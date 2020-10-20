package com.aliyun.spark.ganos

import com.aliyun.ganos.dla._
import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.raster.geotiff._
import org.apache.spark.sql._

/**
  *  DLA Ganos波段代数计算案例，用户读取RGB与NIR波段，并进行基本的代数运算。
  */
object LocalAlgorithm extends App{

  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster

  import spark.implicits._


  //定义文件名称格式，并加载4个波段的遥感影像
  val st_ = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 4
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
  val df1 = spark.read.ganos.geotiff.load(getClass.getResource("/"+st_.format(bandNumbers(0))).getPath).select($"spatial_key",$"tile" as "band_1").asLayer
  val df2 = spark.read.ganos.geotiff.load(getClass.getResource("/"+st_.format(bandNumbers(1))).getPath).select($"spatial_key",$"tile" as "band_2").asLayer
  val df3 = spark.read.ganos.geotiff.load(getClass.getResource("/"+st_.format(bandNumbers(2))).getPath).select($"spatial_key",$"tile" as "band_3").asLayer
  val df4 = spark.read.ganos.geotiff.load(getClass.getResource("/"+st_.format(bandNumbers(3))).getPath).select($"spatial_key",$"tile" as "band_4").asLayer

  //将4个Tile图层合并为MultibandTile图层，以方便分析
  val df=df1 spatialJoin df2 spatialJoin df3 spatialJoin df4
  df.show

  //波段计算
  val addDF = df.withColumn("1+2", st_local_add(df("band_1"), df("band_2")))
    .withColumn("1/2", st_local_divide(df("band_1"), df("band_2")))
    .select(df("spatial_key"),df("band_1"),df("band_2"),$"1+2",$"1/2")
    .asLayer
  addDF.show

  val raster = addDF.select(st_tile_sum(addDF("1+2")),addDF("band_1"),addDF("band_2"))
  raster.show

}
