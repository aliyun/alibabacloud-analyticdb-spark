package com.aliyun.spark.ganos.datasource
import com.aliyun.ganos.dla.geometry.{GanosGeometrySpark, GanosSparkKryoRegistrator}
import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.geotools.data.DataStoreFinder
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.util.factory.Hints
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query, _}
import org.geotools.util.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraDataStoreFactory}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.LooseBBoxParam
import org.locationtech.geomesa.index.utils.{ExplainString, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 *  Cassandra 数据源测试案例
 *
 *  测试前请先运行ingestPoints函数导入测试数据
 *
 */
object CassandraDemo extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("com").setLevel(Level.ERROR)

  //导入测试数据
  ingestPoints

  // 1. 初始化SparkSession
  implicit val spark = SparkSession.builder()
    .master("local[*]")
    .appName(getClass.getName)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", classOf[GanosSparkKryoRegistrator].getName)
    .config("spark.kryoserializer.buffer.max", "2047m")
    .getOrCreate()

  // 2.增建Ganos Geom/Raster扩展
  spark.withGanosGeometry

  val dsParams = Map(
    "cassandra.contact.point" -> "localhost:9042",
    "cassandra.keyspace" -> "ganos",
    "cassandra.catalog"->"test_sft"
  )

  implicit val sc=spark.sparkContext

  val rdd = GanosGeometrySpark(dsParams).rdd(new Configuration(), sc, dsParams, new Query("testpoints"))
  println(rdd.count)

  val df = spark.read
    .format("ganos-geometry")
    .options(dsParams)
    .option("ganos.feature", "testpoints")
    .load()
  df.show
  df.createOrReplaceTempView("testpoints")

  val points = spark.sql("select * from testpoints where st_contains(st_makeBox2d(st_point(38,48), st_point(52,62)),geom)")
  points.printSchema()
  points.show

  points.createOrReplaceTempView("points")
  val buffer = spark.sql("select name,dtg,ST_BufferPoint(geom,10) as geom from points")
  buffer.printSchema()
  buffer.show

  import com.aliyun.ganos.dla.geometry.util.SparkUtils
  val buffer_sft=SparkUtils.structType2SFT(buffer.schema,"buffer")
  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[CassandraDataStore]
  ds.createSchema(buffer_sft)

  buffer.write.format("ganos-geometry").options(dsParams).option("ganos.feature", "buffer").save()
  val resultFrame = spark.read
    .format("ganos-geometry")
    .options(dsParams)
    .option("ganos.feature", "buffer")
    .load()
  resultFrame.printSchema()
  resultFrame.show

  // 随机导入点数据
  def ingestPoints(): Unit ={
    val host = "localhost"
    val port = "9042"
    val cluster = new Cluster.Builder().addContactPoints(host).withPort(port.toInt)
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(10000)).build().init()
    val session = cluster.connect()
    var params = Map(
      Params.ContactPointParam.getName -> s"$host:$port",
      Params.KeySpaceParam.getName -> "ganos",
      Params.CatalogParam.getName -> "test_sft"
    )
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[CassandraDataStore]
    val typeName = "testpoints"

    //ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))
    val sft = ds.getSchema(typeName)
    println(sft)
    val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
    val toAdd = (0 until 10).map { i =>
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      sf.setAttribute(0, s"name$i")
      sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
      sf.setAttribute(2, s"POINT(4$i 5$i)")
      sf
    }
    //val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))

    val filter="bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z"
    val query = new Query(typeName, ECQL.toFilter(filter))
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    println(features)
    features.foreach(println(_))
  }

}
