# 基于GeoMesa实现数据加载与分析

DLA Ganos内置了GeoMesa数据源驱动，可直接对接基于GeoMesa管理时空数据的Cassandra数据源。DLA Ganos内置的GeoMesa为3.X版本，支持Cassandra 3.0以上的版本。
本案例展示如何基于DLA Ganos实现通过SparkSQL查询Cassandra中的时空数据，并将结果写回Cassandra数据库中。

## 1. 初始化数据库
首先需要在工程的pom.xml中导入所需要的依赖：

```xml
<dependency>
        <groupId>com.aliyun.ganos</groupId>
        <artifactId>dla-ganos-sdk</artifactId>
    <version>1.0</version>
</dependency>
<dependency>
        <groupId>org.locationtech.geomesa</groupId>
    <artifactId>geomesa-cassandra-datastore_${scala.binary.version}</artifactId>
    <version>${geomesa.version}</version>
</dependency>
<dependency>
        <groupId>com.datastax.cassandra</groupId>
        <artifactId>cassandra-driver-core</artifactId>
    <version>${cassandra.version}</version>
</dependency>
<dependency>
        <groupId>com.datastax.cassandra</groupId>
        <artifactId>cassandra-driver-mapping</artifactId>
    <version>${cassandra.version}</version>
</dependency>
```
然后初始化Cassandra连接环境：
```scala
var params: Map[String, String] = _
val host = "Cassandra数据库地址"
val port = "9042"
val cluster = new Cluster.Builder().addContactPoints(host).withPort(port.toInt)
    .withSocketOptions(new SocketOptions().setReadTimeoutMillis(10000)).build().init()
val session = cluster.connect()
//配置连接参数
params = Map(
  Params.ContactPointParam.getName -> s"$host:$port",
  Params.KeySpaceParam.getName -> "ganos",
  Params.CatalogParam.getName -> "test_sft"
)
```
输出化数据库连接完成后，可以创建时空表并导入数据。这里我们创建一个名为testpoints的表，该表还有一个时间字段(dtg)和一个空间字段(geom:Point):
```scala
//创建DataStore用于数据写入
val ds = DataStoreFinder.getDataStore(params).asInstanceOf[CassandraDataStore]
val typeName = "testpoints"

//创建表
ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))
val sft = ds.getSchema(typeName)

//写入10个点
val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
val toAdd = (0 until 10).map { i =>
   val sf = new ScalaSimpleFeature(sft, i.toString)
   sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
   sf.setAttribute(0, s"name$i")
   sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
   sf.setAttribute(2, s"POINT(4$i 5$i)")
   sf
}
```
然后我们通过Cassandra的CQL工具查看创建的表和写入的数据，注意表名称为
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/cassandra1.png"></img>
##2. DLA Ganos进行时空查询
```scala
import com.aliyun.ganos.dla.geometry._
import com.aliyun.ganos.dla.geomesa._
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.Query

import scala.collection.JavaConversions._

object CassandraSparkTest extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)

  //初始化SparkSession
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", classOf[GanosSparkKryoRegistrator].getName)
    .config("spark.kryoserializer.buffer.max", "2047m")
    .getOrCreate()

  //增建Ganos Geometry扩展
  spark.withGanosGeometry

  //配置连接参数
  val dsParams = Map(
    "cassandra.contact.point" -> "Cassandra连接地址:9042",
    "cassandra.keyspace" -> "ganos",
    "cassandra.catalog"->"test_sft"
  )
  implicit val sc=spark.sparkContext
  
  //创建DataFrame对应到Cassandra表
  val df = spark.read
    .format("ganos-geometry")
    .options(dsParams)
    .option("ganos.feature", "testpoints")
    .load()
  df.show
  df.createOrReplaceTempView("testpoints")

  //创建SQL查询
  val points = spark.sql("select * from testpoints where st_contains(st_makeBox2d(st_point(38,48), st_point(52,62)),geom)")
  
  //输出Schema与表内容
  points.printSchema
  points.show

}
```
结果输出如下所示：
```text
root
 |-- __fid__: string (nullable = false)
 |-- name: string (nullable = true)
 |-- dtg: timestamp (nullable = true)
 |-- geom: point (nullable = true)
+-------+-----+-------------------+-------------+
|__fid__| name|                dtg|         geom|
+-------+-----+-------------------+-------------+
|      9|name9|2014-01-10 08:00:01|POINT (49 59)|
|      2|name2|2014-01-03 08:00:01|POINT (42 52)|
|      3|name3|2014-01-04 08:00:01|POINT (43 53)|
|      1|name1|2014-01-02 08:00:01|POINT (41 51)|
|      5|name5|2014-01-06 08:00:01|POINT (45 55)|
|      8|name8|2014-01-09 08:00:01|POINT (48 58)|
|      4|name4|2014-01-05 08:00:01|POINT (44 54)|
|      0|name0|2014-01-01 08:00:01|POINT (40 50)|
|      6|name6|2014-01-07 08:00:01|POINT (46 56)|
|      7|name7|2014-01-08 08:00:01|POINT (47 57)|
+-------+-----+-------------------+-------------+
```

## 3. 计算结果写入Cassandra
DLA Ganos同样支持数据写回到Cassandra数据库。我们紧接前面代码，对查询出的points表调用ST_BufferPoint函数，对每个点生成缓冲区，然后写入到Cassandra表中。关于ST_BufferPoint详细解释可以参考时空几何函数参考。
```scala
points.createOrReplaceTempView("points")
val buffer = spark.sql("select name,dtg,ST_BufferPoint(geom,10) as geom from points")
buffer.printSchema()
buffer.show
```
结果输出如下，可以看到每个point对象生成了一个Polygon类型的缓冲区对象：

```text
root
 |-- name: string (nullable = true)
 |-- dtg: timestamp (nullable = true)
 |-- geom: polygon (nullable = true)
+-----+-------------------+--------------------+
| name|                dtg|                geom|
+-----+-------------------+--------------------+
|name0|2014-01-01 08:00:01|POLYGON ((40.0001...|
|name1|2014-01-02 08:00:01|POLYGON ((41.0001...|
|name2|2014-01-03 08:00:01|POLYGON ((42.0001...|
|name3|2014-01-04 08:00:01|POLYGON ((43.0001...|
|name4|2014-01-05 08:00:01|POLYGON ((44.0001...|
|name5|2014-01-06 08:00:01|POLYGON ((45.0001...|
|name6|2014-01-07 08:00:01|POLYGON ((46.0001...|
|name7|2014-01-08 08:00:01|POLYGON ((47.0001...|
|name9|2014-01-10 08:00:01|POLYGON ((49.0001...|
|name8|2014-01-09 08:00:01|POLYGON ((48.0001...|
+-----+-------------------+--------------------+
```

下面我们将buffer对象写回到数据库，并查询：
注意：通过DLA Ganos将DataFrame写回到Cassandra数据库前，必须提前创建好待写入的表，否则将写入失败。如果用户还未创建表，可以参考下面代码创建新表：
```scala
import com.aliyun.ganos.dla.geometry.util.SparkUtils

  //调用SparkUtils的structType2SFT方法将DataFrame的StructType转换为GeoTools的SimpleFeatureType：
  val buffer_sft=SparkUtils.structType2SFT(buffer.schema,"buffer")
  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[CassandraDataStore]
  ds.createSchema(buffer_sft)
```
查询数据库，发现新创建了一个名为buffer的表，内容为空：
<img align="center" height="200px" src="https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/cassandra2.png"></img>

成功创建表之后，就可以写入数据了。这里我们将刚刚生成的buffer表写入：

```scala
//buffer写入到Cassandra的buffer表中
buffer.write.format("ganos-geometry").options(dsParams).option("dla.feature", "buffer").save()

//查询写入结果：
val resultFrame = spark.read
    .format("ganos-geometry")
    .options(dsParams)
    .option("ganos.feature", "buffer")
    .load()
  resultFrame.printSchema()
  resultFrame.show
```
输出如下，可以看到我们通过ST_BufferPoint生成的新表已经成功写入到Cassandra中：

```text
root
 |-- __fid__: string (nullable = false)
 |-- name: string (nullable = true)
 |-- dtg: timestamp (nullable = true)
 |-- geom: polygon (nullable = true)
+--------------------+-----+-------------------+--------------------+
|             __fid__| name|                dtg|                geom|
+--------------------+-----+-------------------+--------------------+
|0000175c-7227-4f1...|name6|2014-01-07 08:00:01|POLYGON ((46.0001...|
|0000175c-7227-4e8...|name7|2014-01-08 08:00:01|POLYGON ((47.0001...|
|0000175c-7227-4ea...|name2|2014-01-03 08:00:01|POLYGON ((42.0001...|
|0000175c-7227-4ea...|name4|2014-01-05 08:00:01|POLYGON ((44.0001...|
|0000175c-7227-4e7...|name1|2014-01-02 08:00:01|POLYGON ((41.0001...|
|0000175c-7227-4ec...|name3|2014-01-04 08:00:01|POLYGON ((43.0001...|
|0000175c-7227-4e7...|name5|2014-01-06 08:00:01|POLYGON ((45.0001...|
|0000175c-7227-4ee...|name0|2014-01-01 08:00:01|POLYGON ((40.0001...|
|0000175c-7227-4f3...|name8|2014-01-09 08:00:01|POLYGON ((48.0001...|
|0000175c-7227-4f3...|name9|2014-01-10 08:00:01|POLYGON ((49.0001...|
+--------------------+-----+-------------------+--------------------+
```
