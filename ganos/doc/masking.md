# Masking
<p>掩膜(Masking)是栅格处理中的常见操作，它将某些单元格设置为NoData值。掩膜操作一般有两个目的，首先是为了从栅格处理中删除低质量的观测值，实现去噪的目的；另一个是按照给定的形状将多边形“剪切”到指定形状，如下图所示：</p>
<p>本案例中我们将使用Landsat影像进行masking操作，下图中左侧为测试数据RGB合成图像，右侧为参与masking操作的数据。</p>
 
<img align="center" height="400px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/masking1.png"></img>

### 第1步：加载数据
<p>首先我们依次加载Red、Green、Blue和NIR四个波段，并合并为一个DataFrame对象。这里我们使用geotrellis提供的SinglebandGeoTiff类来加载单个Band影像，并通过DLA Ganos提供的toLayer(Band_Name)方法转换为DataFrame，最后通过spatialJoin时空算子合并为一个DF：</p>

```scala
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
//加载单波段影像文件
def readTiff(name: String): SinglebandGeoTiff = SinglebandGeoTiff(getClass.getResource(s"/$name").getPath)
//定义文件名称
val filenamePattern = "L8-B%d-Elkton-VA.tiff"
val bandNumbers = 1 to 4
val bandColNames = bandNumbers.map(b ⇒ s"band_$b").toArray
//加载数据并合并
val joinedDF = bandNumbers.
    map { b ⇒ (b, filenamePattern.format(b)) }.
    map { case (b, f) ⇒ (b, readTiff(f)) }.
    map { case (b, t) ⇒ t.projectedRaster.toLayer(s"band_$b") }.
    reduce(_ spatialJoin _)
  joinedDF.show
```
 
<p>输出结果如下图，可以看到该图层加载后只有一个Tile对象，band1到band4分别对应了蓝、绿、红和近红外4个波段。</p>

```text
+-----------+--------------------+--------------------+--------------------+--------------------+
|spatial_key|              band_1|              band_2|              band_3|              band_4|
+-----------+--------------------+--------------------+--------------------+--------------------+
|     [0, 0]|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|ArrayTile(186,169...|
+-----------+--------------------+--------------------+--------------------+--------------------+
```

### 第2步：定义Masking瓦片
<p>接着我们定义一个阈值函数threshold，具体规则为，只保留大于10500的像素值，小于等于10500的像素则以NODATA表示。该函数作用于band_1波段生成一个用于进行masking的Tile对象withMaskedTile。</p>

```scala
val threshold = udf((t: Tile) => {
   t.convert(IntConstantNoDataCellType).map(x => if (x > 10500) x else NODATA)
})
val withMaskedTile = joinedRF.withColumn("maskTile", threshold(joinedDF("band_1"))).asLayer
```

### 第3步：Masking操作
<p>我们调用st_mask函数，使用withMaskedTile对band2进行掩膜操作：</p>

```scala
val masked = withMaskedTile.withColumn("masked", st_mask(joinedDF("band_2"), withMaskedTile("maskTile"))).asLayer
```

### 第4步：结果输出
```scala
val maskDF = masked.toRaster(masked("masked"), 466, 428)
val b2 = masked.toRaster(masked("band_2"), 466, 428)
val brownToGreen = ColorRamp(
    RGB(166,97,26),
    RGB(223,194,125),
    RGB(245,245,245),
    RGB(128,205,193),
    RGB(1,133,113)
).stops(128)
val colors = ColorMap.fromQuantileBreaks(maskDF.tile.histogramDouble(), brownToGreen)
maskDF.tile.color(colors).renderPng().write("mask.png")
b2.tile.color(colors).renderPng().write("b2.png")
```
### 输出结果:
<p>下图从左到右依次为RGB合成图像、Band2图像和masking输出图像。通过对比输出结果可以看到，部分城市建筑像素被保留，其他全部变成了NoData。</p>
<p><img height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/masking2.png"></img>
<img height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/masking3.png"></img>
<img height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/masking4.png"></img></p>
