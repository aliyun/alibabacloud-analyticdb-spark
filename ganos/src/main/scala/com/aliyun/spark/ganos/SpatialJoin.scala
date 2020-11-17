package com.aliyun.spark.ganos

import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.raster.util.Render
import geotrellis.raster._
import com.aliyun.ganos.dla._
import org.apache.spark.sql._

/**
  * 基于某一组遥感影像数据进行Raster Join操作。
  * 该操作将基于CRS将每个DataFrame中的Tile列执行空间连接操作。
  * 默认情况下是左连接，并使用“交“运算符，右侧的所有Tile列会匹配左侧的Tile列的CRS、范围和分辨率等。
  */
object SpatialJoin extends App{

  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster

  import spark.implicits._

  //定义Join图层
  val df1 = spark.read.ganosRaster.load(getClass.getResource("/LC08_L1TP_121035_20190702_20190706_01_T1.TIF").getPath).asLayer
  val df2 = spark.read.ganosRaster.load(getClass.getResource("/srtm_60_05.tif").getPath).asLayer

  df1.show
  df2.show

  //进行空间Join
  val joinDF=df1.rasterJoin(df2)
  //val df=joinDF.select($"spatial_key",st_rgb_composite($"tile_3",$"tile_2",$"tile_1"),$"tile")
  joinDF.show

  joinDF.cache()
  val row=joinDF.select($"tile_1",$"tile_2",$"tile_3",$"tile").rdd.take(30)

  //将结果按照Tile输出到文件夹
  var i=0
  row.foreach(p=>{
    val tile1=p.get(0).asInstanceOf[Tile]
    val tile2=p.get(1).asInstanceOf[Tile]
    val tile3=p.get(2).asInstanceOf[Tile]
    val srtm_tile=p.get(3).asInstanceOf[Tile]
    val tile=MultibandTile(Seq(tile1,tile2,tile3))
    Render.image(tile).write(s"tile_${i}.png")
    val colorMap = ColorMap(
      (-2 to 800).toArray,
      ColorRamps.HeatmapBlueToYellowToRedSpectrum
    )
    srtm_tile.renderPng(colorMap).write(s"join/srtm_${i}.png")
    i=i+1
  })
}
