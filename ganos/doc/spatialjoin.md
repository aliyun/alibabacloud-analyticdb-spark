# 多源异构栅格Join
<p>在复杂的业务系统中，往往需要对多个数据源进行联邦分析。这些数据具有不同的投影信息与分辨率，所以在分析之前需要对数据进行重投影与重采样等转换操作。在许多使用情况下，这种转换操作都是基于某一组遥感影像数据进行的。在DLA Ganos中，可以对多源栅格数据类型DataFrame执行Raster Join操作。该操作将基于CRS将每个DataFrame中的Tile列执行空间连接操作。默认情况下是左连接，并使用“交“运算符，右侧的所有Tile列会匹配左侧的Tile列的CRS、范围和分辨率等。</p>

<p>本示例中我们将展示对两种不同的遥感数据（Landsat8和SRTM）执行Join操作。</p>

### 第1步：初始化SparkSession
```scala
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster //加载DLA Ganos Raster时空插件
  import spark.implicits._
  ```
### 第2步：加载Landsat和SRTM数据
<p>通过 spark.read.ganos.geotiff加载本地的Landsat和SRTM数据：</p>

```scala
 val landsatDF = spark.read.ganos.geotiff.load(getClass.getResource("/LC08_L1TP_121035_20190702_20190706_01_T1.TIF").getPath).asLayer
 val srtmDF = spark.read.ganos.geotiff.load(getClass.getResource("/srtm_60_05.tif").getPath).asLayer
```
<p>LandsatDF输出:</p>
<img align="center" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join1.png"></img>
<p>srtmDF输出:</p>
<img align="center" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join2.png"></img>

### 第3步：执行RasterJoin操作：
```scala
val joinDF=landsatDF.rasterJoin(srtmDF)
joinDF.show
```
<p>Join之后的DataFrame输入如下，可以看到landsatDF和srtmDF按照spatial_key连接在了一起：</p>
<img align="center" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join3.png"></img>

<p>将Join后的Landsat和SRTM Tile对象输出为PNG后对比效果如下：</p>

| spatial_key | Landsat RGB | SRTM |
| ------- | ------- | ------- |
| [0, 10] | <img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join1_1.png"></img>| <img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join1_2.png"></img> |
| [14, 19] | <img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join2_1.png"></img>| <img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join2_2.png"></img>|
| [18, 8] | <img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join3_1.png"></img>|<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/join3_2.png"></img> |
