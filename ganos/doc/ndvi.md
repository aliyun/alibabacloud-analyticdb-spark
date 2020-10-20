#自定义UDF
<p>用户可以通过自定义UDF来扩展DLA Ganos的功能，从而更加方便地与业务系统对接。这里还是以NDVI为例，来展示如何基于DLA Ganos实现用户自定义的UDF算子.</p>

### 第1步：初始化SparkSession与DLA Ganos

```scala
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster
  import spark.implicits._
  ```
### 第2步：加载栅格数据图层

```scala
def readTiff(name: String) =
    SinglebandGeoTiff(IOUtils.toByteArray(getClass.getResourceAsStream(s"/$name")))
def redBand = readTiff("L8-B4-Elkton-VA.tiff").projectedRaster.toLayer("red_band")
def nirBand = readTiff("L8-B5-Elkton-VA.tiff").projectedRaster.toLayer("nir_band")
```

### 第3步：自定义UDF算子

```scala
 val ndvi = udf((red: Tile, nir: Tile) => {
    val redd = red.convert(DoubleConstantNoDataCellType)
    val nird = nir.convert(DoubleConstantNoDataCellType)
    (nird - redd) / (nird + redd)
  })
 ```
### 第4步：计算NDVI并输出结果

```scala
val df = redBand.spatialJoin(nirBand).withColumn("ndvi", ndvi($"red_band", $"nir_band")).asLayer
val pr = df.toRaster($"ndvi", 233, 214)
val brownToGreen = ColorRamp(
    RGB(166, 97, 26),
    RGB(223, 194, 125),
    RGB(245, 245, 245),
    RGB(128, 205, 193),
    RGB(1, 133, 113)
  ).stops(128)
val colors = ColorMap.fromQuantileBreaks(pr.tile.histogramDouble(), brownToGreen)
pr.tile.color(colors).renderPng().write("ndvi.png")
```
### 结果输出如下:
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/ndvi_1.png"></img>
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/ndvi_2.png"></img>
