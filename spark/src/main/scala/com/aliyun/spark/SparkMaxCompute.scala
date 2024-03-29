package com.aliyun.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
object SparkMaxCompute {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      // scalastyle:off
      System.err.println(
        """Usage: TestOdps <accessKeyId> <accessKeySecret> <envType> <project> <table> <numPartitions>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    envType          0 or 1
          |                     0: Public environment.
          |                     1: Aliyun internal environment, i.e. Aliyun ECS etc.
          |    project          Aliyun ODPS project
          |    table            Aliyun ODPS table
          |    numPartitions    the number of RDD partitions
        """.stripMargin)
      // scalastyle:on
      System.exit(1)
    }

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val envType = args(2).toInt
    val project = args(3)
    val table = args(4)

    val urls = Seq(
      // public environment
      Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"),
      // Aliyun internal environment
      Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com")
    )

    val odpsUrl = urls(envType)(0)
    val tunnelUrl = urls(envType)(1)

    val ss = SparkSession.builder().appName("Test Odps Read").master("local[*]").getOrCreate()

    import ss.implicits._

    val dataSeq = (1 to 1000000).map {
      index => (index, (index-3).toString)
    }.toSeq


    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    // scalastyle:off
    System.out.println("*****" + table + ",before overwrite table")
    // scalastyle:on
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    // scalastyle:off
    System.out.println("*****" + table + ",after overwrite table, before read table")
    // scalastyle:on

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()


    val collectList = readDF.collect()
    // scalastyle:off
    System.out.println("*****" + table + ",after read table," + collectList.size)
    // scalastyle:on
    assert(collectList.length == 1000000)
    assert((1 to 1000000).par.exists(n => collectList.exists(_.getString(0) == n+"")))
  }
}
