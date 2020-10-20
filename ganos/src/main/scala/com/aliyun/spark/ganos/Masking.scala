package com.aliyun.spark.ganos

import com.aliyun.ganos.dla.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.{mask => _, _}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * DLA Ganos 栅格数据Masking操作
  */
object Masking extends App {

  implicit val spark = SparkSession.builder().
    master("local[*]").appName(getClass.getName).
    config("spark.ui.enabled", "false").
    getOrCreate().
    withGanosRaster

  //定义文件名称格式，并加载4个波段的遥感影像
  def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(getClass.getResource(s"/$name").getPath)
  val filenamePattern = "L8-B%d-Elkton-VA.tiff"
  val bandNumbers = 1 to 4
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
  val joinedRF = bandNumbers.
    map { b ⇒ (b, filenamePattern.format(b)) }.
    map { case (b, f) ⇒ (b, readTiff(f)) }.
    map { case (b, t) ⇒ t.projectedRaster.toLayer(s"band_$b") }.
    reduce(_ spatialJoin _)
  joinedRF.show

  //定义阈值进行像素过滤
  val threshold = udf((t: Tile) => {
    t.convert(IntConstantNoDataCellType).map(x => if (x > 10500) x else NODATA)
  } )

  //定义进行Mask操作的图层，这里选择band_1
  val withMaskedTile = joinedRF.withColumn("maskTile", threshold(joinedRF("band_1"))).asLayer
  withMaskedTile.show

  //通过st_no_data_cells udf选择空像元
  withMaskedTile.select(st_no_data_cells(withMaskedTile("maskTile"))).show()

  //对波段2图层进行mask操作
  val masked = withMaskedTile.withColumn("masked", st_mask(joinedRF("band_2"), withMaskedTile("maskTile"))).asLayer
  val maskRF = masked.toRaster(masked("masked"), 466, 428)
  val b2 = masked.toRaster(masked("band_2"), 466, 428)

  val brownToGreen = ColorRamp(
    RGB(166,97,26),
    RGB(223,194,125),
    RGB(245,245,245),
    RGB(128,205,193),
    RGB(1,133,113)
  ).stops(128)

  //渲染并输出结果
  val colors = ColorMap.fromQuantileBreaks(maskRF.tile.histogramDouble(), brownToGreen)
  maskRF.tile.color(colors).renderPng().write("mask.png")
  b2.tile.color(colors).renderPng().write("b2.png")
  spark.stop()
}
