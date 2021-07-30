package com.aliyun.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
//https://help.aliyun.com/document_detail/187100.html?spm=a2c4g.11186623.2.26.25336939JXk48g#topic-1962927
object SparkTableStore {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        """Usage: SparkTableStore <instance-id> <table-name> <实例访问地址-VPC> <ACCESS_KEY_ID>
          |         <ACCESS_KEY_SECRET>
        """.stripMargin)
      System.exit(1)
    }
    val instanceId = args(0)
    val tableName = args(1)
    val endpoint = args(2)
    val accessKeyId = args(3)
    val accessKeySecret = args(4)
    //表结构,您需要在tablestore中准备具有如下表结构的表
    val catalog =
      """
        |{
        |  "columns": {
        |    "id":{
        |      "col":"id",
        |      "type":"string"
        |    },
        |     "company": {
        |      "col": "company",
        |      "type": "string"
        |    },
        |    "age": {
        |      "col": "age",
        |      "type": "integer"
        |    },
        |
        |    "name": {
        |      "col": "name",
        |      "type": "string"
        |    }
        |  }
        |}
        |""".stripMargin



    val sparkConf = new SparkConf
    val options = new mutable.HashMap[String, String]()
    //您的tablestore实例名称
    options.put("instance.name", instanceId)

    //您想要连接表名
    options.put("table.name", tableName)

    //您的访问的endpoint
    options.put("endpoint", endpoint)

    //您的ACCESS KEY
    options.put("access.key.id", accessKeyId)

    //您的Key SECRET
    options.put("access.key.secret", accessKeySecret)

    //您的表格结构，使用json表达式
    options.put("catalog", catalog)

    //与Long类型做Range( >= > < <= )比较的谓词是否下推
    options.put("push.down.range.long", "true")
    //与String类型做Range( >= > < <= )比较的谓词是否下推
    options.put("push.down.range.string", "true")


    // Tablestore通道 Channel在每个Spark Batch周期内同步的最大数据条数，默认10000。
    options.put("maxOffsetsPerChannel", "10000")
    //多元索引名,可选
    //options.put("search.index.name", "<index_name>")
    //tunnel id，可选
    //options.put("tunnel.id", "<tunnel id>")
    val spark = SparkSession.builder.config(sparkConf).appName("Serverless Spark TableStore Demo").getOrCreate
    val dfReader = spark.read.format("tablestore").options(options)
    val df = dfReader.load()
    //显示表内容
    df.show()
    df.printSchema()

    //写表
    val schema = StructType.apply(Seq(StructField("id", StringType), StructField("company", StringType),StructField("age", IntegerType), StructField("name", StringType)))
    val newData = spark.sparkContext.parallelize(Seq(("1","ant",10,"xxx"))).map(row => Row(row._1, row._2, row._3, row._4))
    val newDataDF = spark.createDataFrame(newData, schema)
    newDataDF.write.format("tablestore").options(options).save
    val dfReader1 = spark.read.format("tablestore").options(options)
    val df1 = dfReader1.load()
    df1.show()
  }
}
