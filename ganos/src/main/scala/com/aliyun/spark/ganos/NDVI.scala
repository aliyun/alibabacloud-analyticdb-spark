/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2017 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.aliyun.spark.ganos

import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.raster.geotiff._
import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * 自定义UDF，计算NDVI
  */
object NDVI extends App {

  def readTiff(name: String) =
    SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream(s"/$name")))

  implicit val spark = SparkSession
    .builder()
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
  val df1 = spark.read.geotiff.load(getClass.getResource("/" + st_.format(bandNumbers(0))).getPath).select($"spatial_key", $"tile" as "band_1").asLayer
  val df2 = spark.read.geotiff.load(getClass.getResource("/" + st_.format(bandNumbers(1))).getPath).select($"spatial_key", $"tile" as "band_2").asLayer
  val df3 = spark.read.geotiff.load(getClass.getResource("/" + st_.format(bandNumbers(2))).getPath).select($"spatial_key", $"tile" as "band_3").asLayer
  val df4 = spark.read.geotiff.load(getClass.getResource("/" + st_.format(bandNumbers(3))).getPath).select($"spatial_key", $"tile" as "band_4").asLayer
  val df = df1 spatialJoin df2 spatialJoin df3 spatialJoin df4

  val ndvi = udf((red: Tile, nir: Tile) => {
    val redd = red.convert(DoubleConstantNoDataCellType)
    val nird = nir.convert(DoubleConstantNoDataCellType)
    (nird - redd) / (nird + redd)
  })

  //调用用户自定义的NDVI计算
  val rf = df.withColumn("ndvi", ndvi($"tile_3", $"tile_4")).asLayer
  rf.printSchema()

  //渲染并输出
  val ndviColorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.ndviColormap")).get
  val pr = rf.toRaster($"ndvi", 800, 800)
  println(pr.tile.histogramDouble().minMaxValues().get)
  pr.tile.color(ndviColorMap).renderPng().write("ndvi.png")
}
