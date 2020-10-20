#栅格代数运算
### 栅格代数运算简介
<p>栅格代数运算是指使用数学运算符对栅格数据进行加减乘除等代数计算的操作。例如，您可以应用简单的数学运算（例如加法或乘法）来更新栅格像元值，或者对多个栅格数据图层执行叠加计算（Overlay）等。栅格代数运算中最常见的类型是像素单元函数，即栅格单元直接堆叠在一起进行计算。该功能适用于分辨率相同的栅格图层之间的叠加计算。按照计算方式不同，栅格代数运算主要可以分为以下四类：</p>

| 名称| 图示 |
| ----- | ---- | 
| Local模式 | <img align="center" width="150" height="150" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm1.png"></img> | 
| Global模式 | <img align="center" width="150" height="150" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm2.png"></img> | 
| Focal模式 | <img align="center" width="150" height="150" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm3.png"></img> | 
| Zonal模式 | <img align="center" width="150" height="150"  src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm4.png"></img> |


### 本地(Local)栅格代数简介
DLA Ganos提供了多种本地栅格代数功能。根据参与计算的Tile对象数量，可以分为以下几类：
* 对单个Tile对象进行操作的一元运算；例如：st_log;
* 由两个Tile对象参与的二元运算；例如：st_local_multiply;
* 由一个Tile对象和标量参与的二元运算；例如：st_local_less;
* 多个(大于2)Tile对象参与操作的N元运算；例如：st_agg_local_min

### 第1步：初始化

```scala
import com.aliyun.ganos.dla.raster.geotiff._
import org.apache.spark.sql._
import com.aliyun.ganos.dla.raster._
implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .withKryoSerialization
    .getOrCreate()
    .withGanosRaster
import spark.implicits._
```

### 第2步：加载数据
首先我们依次加载Red、Green、Blue和NIR四个波段，并合并为一个DataFrame对象：

```scala
val filenamePattern = "L8-B%d-Elkton-VA.tiff"
val bandNumbers = 1 to 4
val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
val df1 = spark.read.ganos.geotiff.load(getClass.getResource("/"+filenamePattern.format(bandNumbers(0))).getPath).select($"spatial_key",$"tile" as "band_1").asLayer
val df2 = spark.read.ganos.geotiff.load(getClass.getResource("/"+filenamePattern.format(bandNumbers(1))).getPath).select($"spatial_key",$"tile" as "band_2").asLayer
val df3 = spark.read.ganos.geotiff.load(getClass.getResource("/"+filenamePattern.format(bandNumbers(2))).getPath).select($"spatial_key",$"tile" as "band_3").asLayer
val df4 = spark.read.ganos.geotiff.load(getClass.getResource("/"+filenamePattern.format(bandNumbers(3))).getPath).select($"spatial_key",$"tile" as "band_4").asLayer
val df=df1 spatialJoin df2 spatialJoin df3 spatialJoin df4
df.show
```

输出：
```text
+-----------+--------------------+--------------------+--------------------+--------------------+
|spatial_key|              band_1|              band_2|              band_3|              band_4|
+-----------+--------------------+--------------------+--------------------+--------------------+
|     [0, 0]|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|
+-----------+--------------------+--------------------+--------------------+--------------------+
```
### 第3步：波段计算
对band1和band2依次进行加法运算(st_local_add)和除法运算(st_local_divide)，并将计算结果追加到df，列名分别为"1+2"和"1/2":
```scala
 val addDF = df.withColumn("1+2", st_local_add(df("band_1"), df("band_2")))
    .withColumn("1/2", st_local_divide(df("band_1"), df("band_2")))
    .select(df("spatial_key"),df("band_1"),df("band_2"),$"1+2",$"1/2")
    .asLayer
```

结果：
```text
+-----------+--------------------+--------------------+--------------------+--------------------+
|spatial_key|              band_1|              band_2|                 1+2|                 1/2|
+-----------+--------------------+--------------------+--------------------+--------------------+
|     [0, 0]|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|
+-----------+--------------------+--------------------+--------------------+--------------------+
```

最后，对“1+2”和“1/2”进行求和(st_tile_sum)聚合计算：

```scala
val raster = addDF.select(st_tile_sum(addDF("1+2")),addDF("band_1"),addDF("band_2"))
```

输出：
```text
+----------------+--------------------+--------------------+
|rf_tile_sum(1+2)|              band_1|              band_2|
+----------------+--------------------+--------------------+
|    5.89817054E8|ArrayTile(186,169...|ArrayTile(186,169...|
+----------------+--------------------+--------------------+
```

### 计算NDVI
<p>下面我们进行一个稍微复杂一些的计算——归一化植被指数（NDVI）。 NDVI是一种植被指数，检测植被生长状态、植被覆盖度和消除部分辐射误差等。NDVI的代数表达式如下：</p>

```text
       nir - red
NDVI = ---------
       nir + red
```

<p>（band1-band2）/（band1 + band1）的这种形式在遥感学科中很常见，称为归一化指数（normalized difference），它可以与其他波段一起使用以突出水，雪和其他现象。</p>
<p>（此处使用数据为前面已经加载的df表，首先通过st_local_subtract，st_local_add和st_local_divide三个local栅格代数算子计算NDVI：</p>
 
 ```scala
 val ndviDF = df.withColumn("ndvi", 
              st_local_divide(st_local_subtract(df("band_1"), df("band_2")), 
                              st_local_add(df("band_1"), df("band_2")))).asLayer
 ```
 
然后输出结果：
```scala
val pr = ndviDF.toRaster($"ndvi", 233, 214)
pr.tile.color(colors).renderPng().write("df-ndvi.png")
```

<p>原始结果与计算结果示意图如下，可以看到绿色地区为计算出的植被像素值。
### 结果输出如下:
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm5.png"></img>
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/algorithm6.png"></img>
