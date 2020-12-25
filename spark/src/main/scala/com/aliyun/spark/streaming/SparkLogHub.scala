package com.aliyun.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
//说明 默认配置下，Receiver模式在异常情况下可能导致数据丢失。为了避免此类情况发生，建议开启Write-Ahead Logs开关（Spark 1.2以上版本支持）
object SparkLogHub {
  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      System.err.println(
        """Usage: LoghubSample <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds> <checkpoint dir>
        """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)
    val checkPointDir = args(7)
    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("LoghubSample")
      val ssc = new StreamingContext(conf, batchInterval)
      val loghubStream = LoghubUtils.createStream(
        ssc,
        loghubProject,
        logStore,
        loghubGroupName,
        endpoint,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK)

      loghubStream.checkpoint(batchInterval * 2).foreachRDD(rdd => println(rdd.count()))
      ssc.checkpoint(checkPointDir) // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkPointDir, functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
