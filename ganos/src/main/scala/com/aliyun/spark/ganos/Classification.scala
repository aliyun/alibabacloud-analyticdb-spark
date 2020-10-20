/*
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2020 Astraea, Inc.
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
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package com.aliyun.spark.ganos

import com.aliyun.ganos.dla.raster._
import com.aliyun.ganos.dla.raster.ml.{NoDataFilter, TileExploder}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.render.{ColorRamps, IndexedColorMap}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql._

/**
  * 基于DLA Ganos和SparkML进行机器学习————监督分类。
  *
  */
object Classification extends App {

  //读取GeoTiff函数
  def readTiff(name: String) =  GeoTiffReader.readSingleband(getClass.getResource(s"/$name").getPath)

  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster

  import spark.implicits._

  ////加载遥感影像数据
  val filenamePattern = "L8-%s-Elkton-VA.tiff"
  val bandNumbers = 2 to 7
  val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
  val tileSize = 128

  // 分波段进行加载
  val joinedRF = bandNumbers
    .map { b ⇒ (b, filenamePattern.format("B" + b)) }
    .map { case (b, f) ⇒ (b, readTiff(f)) }
    .map { case (b, t) ⇒ t.projectedRaster.toLayer(tileSize, tileSize, s"band_$b") }
    .reduce(_ spatialJoin _)
    .withCRS()
    .withExtent()

  joinedRF.printSchema()

  //加载分类标签
  val targetCol = "target"

  // 加载目标标签。我们必须将单元格类型转换为Double以满足SparkML的要求
  val target = readTiff(filenamePattern.format("Labels"))
    .mapTile(_.convert(DoubleConstantNoDataCellType))
    .projectedRaster
    .toLayer(tileSize, tileSize, targetCol)

  target.show
  // Take a peek at what kind of label data we have to work with.
  target.select(st_agg_stats(target(targetCol))).show

  val abt = joinedRF.spatialJoin(target)

  abt.show

  // SparkML要求每个观察都在其自己的行中，并将这些观察打包到单个“向量”中。第一步是将图块“分解”为每个单元格/像素单行
  val exploder = new TileExploder()

  val noDataFilter = new NoDataFilter().setInputCols(bandColNames :+ targetCol)

  // 为了“向量化”波段列，我们使用SparkML`VectorAssembler`
  val assembler = new VectorAssembler()
    .setInputCols(bandColNames)
    .setOutputCol("features")

  //使用决策树进行分类
  val classifier = new DecisionTreeClassifier()
    .setLabelCol(targetCol)
    .setFeaturesCol(assembler.getOutputCol)

  // 生成Pipeline模型
  val pipeline = new Pipeline()
    .setStages(Array(exploder, noDataFilter, assembler, classifier))

  //  配置如何进行评估模型的性能。
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol(targetCol)
    .setPredictionCol("prediction")
    .setMetricName("f1")

  // 使用参数网格来确定此数据的最佳最大树深度
  val paramGrid = new ParamGridBuilder()
    //.addGrid(classifier.maxDepth, Array(1, 2, 3, 4))
    .build()

  // 配置交叉验证器
  val trainer = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(4)

  // 开始模型匹配
  val model = trainer.fit(abt)

  // 格式化`paramGrid`并设置结果模型
  val metrics = model.getEstimatorParamMaps
    .map(_.toSeq.map(p ⇒ s"${p.param.name} = ${p.value}"))
    .map(_.mkString(", "))
    .zip(model.avgMetrics)
  metrics.toSeq.toDF("params", "metric").show(false)

  // 对原始数据集（包括单元格）分类并进行评分
  val scored = model.bestModel.transform(joinedRF)

  // 输出各个类别的分类统计情况
  scored.groupBy($"prediction" as "class").count().show

  scored.show(20)

  val tlm = joinedRF.tileLayerMetadata.left.get

  val retiled: DataFrame = scored.groupBy($"crs", $"extent").agg(
    st_assemble_tile(
      $"column_index", $"row_index", $"prediction",
      tlm.tileCols, tlm.tileRows, IntConstantNoDataCellType
    )
  )

  val rf: RasterFrameLayer = retiled.toLayer(tlm)

  val raster = rf.toRaster($"prediction", 186, 169)

  val clusterColors = IndexedColorMap.fromColorMap(
    ColorRamps.Viridis.toColorMap((0 until 3).toArray)
  )

  raster.tile.renderPng(clusterColors).write("classified.png")

  spark.stop()
}