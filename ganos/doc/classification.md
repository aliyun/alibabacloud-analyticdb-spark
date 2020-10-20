# 机器学习

<p>本节我们展如如何基于DLA Ganos和SparkML进行机器学习等操作。遥感科学中最常见的一类机器学习操作是监督分类，又称训练分类法。监督分类是用被确认类别的样本像元去识别其他未知类别像元的过程。它就是在分类之前通过目视判读和野外调查，对遥感图像上某些样区中影像地物的类别属性有了先验知识，对每一种类别选取一定数量的训练样本，计算机计算每种训练样区的统计或其他信息，同时用这些种子类别对判决函数进行训练，使其符合于对各种子类别分类的要求，随后用训练好的判决函数去对其他待分数据进行分类。</p>

### 第一步：定义分类样本
首先加载地物类型要素的样本数据：

```json
{
  "type": "FeatureCollection",
  "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
  "features": [
    { "type": "Feature", "properties": { "id": 0 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -78.638383330417554, 38.391061314584398 ], [ -78.637172448825936, 38.395449766458547 ], [ -78.629215101908301, 38.396549823989744 ], [ -78.630786820138638, 38.392028667955493 ], [ -78.634228219464887, 38.390206983429579 ], [ -78.638383330417554, 38.391061314584398 ] ] ] } },
    { "type": "Feature", "properties": { "id": 1 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -78.625973620483947, 38.384086145975928 ], [ -78.619781683426993, 38.388373376437947 ], [ -78.613688820548219, 38.386918574305838 ], [ -78.611752103843472, 38.388980469097866 ], [ -78.612829995997004, 38.39138397517295 ], [ -78.604257166078241, 38.392259658270049 ], [ -78.609485512325875, 38.382139853466676 ], [ -78.625973620483947, 38.384086145975928 ] ] ] } },
    { "type": "Feature", "properties": { "id": 2 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -78.65213631116778, 38.386854807768536 ], [ -78.647983519633271, 38.385930966410982 ], [ -78.650653269705685, 38.383182700685978 ], [ -78.652397953903673, 38.384268357728601 ], [ -78.65213631116778, 38.386854807768536 ] ] ] } }
  ]
}
```

可以看到样本中将地物分为三类，如下图所示：

<img align="center" height="300px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/classification_1.png"></img>

### 第二步：初始化SparkSession

```scala
implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster
import spark.implicits._
```

### 第三步：加载栅格数据：
```scala
val filenamePattern = "L8-%s-Elkton-VA.tiff"
//加载遥感影像数据
val joinedDF = bandNumbers
    .map { b ⇒ (b, filenamePattern.format("B" + b)) }
    .map { case (b, f) ⇒ (b, readTiff(f)) }
    .map { case (b, t) ⇒ t.projectedRaster.toLayer(tileSize, tileSize, s"band_$b") }
    .reduce(_ spatialJoin _)
    .withCRS()
    .withExtent()
//加载分类标签
val target = readTiff(filenamePattern.format("Labels"))
    .mapTile(_.convert(DoubleConstantNoDataCellType))
    .projectedRaster
    .toLayer(tileSize, tileSize, targetCol)
target.select(st_agg_stats(target(targetCol))).show
val abt = joinedDF.spatialJoin(target)
```

joinedDF输出结果如下
```text
+-----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|spatial_key|              band_2|              band_3|              band_4|              band_5|              band_6|              band_7|                 crs|              extent|              target|
+-----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|     [1, 0]|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|[+proj=utm +zone=...|[707814.522176699...|ArrayTile(128,128...|
|     [1, 1]|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|[+proj=utm +zone=...|[707814.522176699...|ArrayTile(128,128...|
|     [0, 0]|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|[+proj=utm +zone=...|[703986.502389, 4...|ArrayTile(128,128...|
|     [0, 1]|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|ArrayTile(128,128...|[+proj=utm +zone=...|[703986.502389, 4...|ArrayTile(128,128...|
+-----------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
```

target输出如下：

```text
+-----------+--------------------+
|spatial_key|              target|
+-----------+--------------------+
|     [0, 0]|ArrayTile(128,128...|
|     [1, 0]|ArrayTile(128,128...|
|     [0, 1]|ArrayTile(128,128...|
|     [1, 1]|ArrayTile(128,128...|
+-----------+--------------------+
```

### 第四步：初始化SparkML分类器：
SparkML要求每个观测值（像素值）都在其自己的Row中，并将这些观测值打包到单个向量(vector)中。

```scala
val exploder = new TileExploder()
val noDataFilter = new NoDataFilter().setInputCols(bandColNames :+ targetCol)
val assembler = new VectorAssembler().setInputCols(bandColNames).setOutputCol("features")
//使用决策树进行分类
val classifier = new DecisionTreeClassifier().setLabelCol(targetCol).setFeaturesCol(assembler.getOutputCol)
//组装模型管道
val pipeline = new Pipeline().setStages(Array(exploder, noDataFilter, assembler, classifier))
// 配置如何进行评估模型的性能。
val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol(targetCol)
    .setPredictionCol("prediction")
    .setMetricName("f1")
//使用参数网格来确定此数据的最佳最大树深度
val paramGrid = new ParamGridBuilder().build()
//配置交叉验证器
val trainer = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(4)
```
运行分类模型，并输出统计参数：
```scala
//开始模型匹配
val model = trainer.fit(abt)
// 格式化`paramGrid`并设置结果模型
val metrics = model.getEstimatorParamMaps
    .map(_.toSeq.map(p ⇒ s"${p.param.name} = ${p.value}"))
    .map(_.mkString(", "))
    .zip(model.avgMetrics)
// 呈现参数/性能关联
metrics.toSeq.toDF("params", "metric").show(false)
// 对原始数据集（包括单元格）分类并进行评分
val scored = model.bestModel.transform(joinedRF)
//输出各个类别的分类统计情况
scored.groupBy($"prediction" as "class").count().show
```

输出如下：
```text
+-----+-----+
|class|count|
+-----+-----+
|  0.0| 7680|
|  1.0|48472|
|  2.0| 9384|
+-----+-----+
```

打印出各个像素值的得分情况：
```scala
scored.show(10)
```

```text
+-----------+--------------------+--------------------+------------+---------+-------+-------+-------+-------+-------+-------+--------------------+--------------+-------------+----------+
|spatial_key|                 crs|              extent|column_index|row_index| band_2| band_3| band_4| band_5| band_6| band_7|            features| rawPrediction|  probability|prediction|
+-----------+--------------------+--------------------+------------+---------+-------+-------+-------+-------+-------+-------+--------------------+--------------+-------------+----------+
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           0|        0|13019.0|12702.0|12218.0|21716.0|16023.0|11693.0|[13019.0,12702.0,...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           1|        0| 9174.0| 9096.0| 7801.0|22141.0|14738.0| 9572.0|[9174.0,9096.0,78...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           2|        0| 9868.0| 9724.0| 8784.0|19613.0|14417.0|10035.0|[9868.0,9724.0,87...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           3|        0| 9141.0| 8869.0| 7913.0|18110.0|14255.0| 9681.0|[9141.0,8869.0,79...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           4|        0| 9343.0| 8758.0| 7875.0|17573.0|12222.0| 8840.0|[9343.0,8758.0,78...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           5|        0| 9556.0| 9034.0| 8271.0|18786.0|13324.0| 9644.0|[9556.0,9034.0,82...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           6|        0| 9279.0| 8847.0| 8026.0|18929.0|12974.0| 9249.0|[9279.0,8847.0,80...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           7|        0| 9488.0| 9141.0| 8370.0|19105.0|13805.0| 9677.0|[9488.0,9141.0,83...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           8|        0| 9560.0| 9054.0| 8321.0|16787.0|12411.0| 9015.0|[9560.0,9054.0,83...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|           9|        0| 8921.0| 8439.0| 7481.0|19336.0|12311.0| 8476.0|[8921.0,8439.0,74...| [2.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          10|        0| 8986.0| 8531.0| 7463.0|21129.0|12896.0| 8869.0|[8986.0,8531.0,74...| [2.0,0.0,0.0]|[1.0,0.0,0.0]|       0.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          11|        0| 9328.0| 8841.0| 7968.0|18993.0|12990.0| 9304.0|[9328.0,8841.0,79...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          12|        0| 9026.0| 8454.0| 7527.0|17611.0|11635.0| 8355.0|[9026.0,8454.0,75...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          13|        0| 9412.0| 8893.0| 8014.0|18349.0|12322.0| 8898.0|[9412.0,8893.0,80...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          14|        0| 9787.0| 9330.0| 8456.0|17029.0|12134.0| 9014.0|[9787.0,9330.0,84...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          15|        0|10079.0| 9646.0| 9009.0|16858.0|12795.0| 9744.0|[10079.0,9646.0,9...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          16|        0| 9522.0| 9018.0| 8282.0|18018.0|12794.0| 9331.0|[9522.0,9018.0,82...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          17|        0| 9225.0| 8884.0| 8175.0|19587.0|13401.0| 9345.0|[9225.0,8884.0,81...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          18|        0| 9548.0| 9248.0| 8583.0|19172.0|13746.0| 9741.0|[9548.0,9248.0,85...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
|     [1, 0]|[+proj=utm +zone=...|[707814.522176699...|          19|        0| 9513.0| 9093.0| 8361.0|18528.0|13648.0| 9608.0|[9513.0,9093.0,83...|[0.0,0.0,99.0]|[0.0,0.0,1.0]|       2.0|
+-----------+--------------------+--------------------+------------+---------+-------+-------+-------+-------+-------+-------+--------------------+--------------+-------------+----------+
```
结果输出：

```scala
val tlm = joinedDF.tileLayerMetadata.left.get
  val retiled: DataFrame = scored.groupBy($"crs", $"extent").agg(
    st_assemble_tile(
      $"column_index", $"row_index", $"prediction",
      tlm.tileCols, tlm.tileRows, IntConstantNoDataCellType
    )
)
val df: RasterFrameLayer = retiled.toLayer(tlm)
val raster = df.toRaster($"prediction", 186, 169)
val clusterColors = IndexedColorMap.fromColorMap(
   ColorRamps.Viridis.toColorMap((0 until 3).toArray)
)
raster.tile.renderPng(clusterColors).write("classified.png")
```
### 原始影像与最终分类结果如下:
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/classification_2.png"></img>
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/classification_3.png"></img>

