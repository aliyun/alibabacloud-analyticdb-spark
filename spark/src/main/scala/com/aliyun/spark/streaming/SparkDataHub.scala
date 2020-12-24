package com.aliyun.spark.streaming

import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.aliyun.datahub.{CanCommitOffsets, DatahubUtils}

object SparkDataHub {
  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      // scalastyle:off
      println(
        """
          |Usage: TestDirectDatahub endpoint project topic subId accessId accessKey duration <zookeeper-host:port> <checkpointdir>
        """.stripMargin)
      // scalastyle:on
      sys.exit(1)
    }
    val endpoint = args(0)
    val project = args(1)
    val topic = args(2)
    val subId = args(3)
    val accessId = args(4)
    val accessKey = args(5)
    val duration = args(6).toLong * 1000
    val zkServer = args(7)
    val checkpointDir = args(8)
    val zkParam = Map("zookeeper.connect" -> zkServer)

    def getStreamingContext(): StreamingContext = {
      val sc = new SparkContext(new SparkConf().setAppName("test-direct-datahub"))
      val ssc = new StreamingContext(sc, Duration(duration))
      val dstream = DatahubUtils.createDirectStream(
        ssc,
        endpoint,
        project,
        topic,
        subId,
        accessId,
        accessKey,
        read(_),
        zkParam)

      dstream.checkpoint(Duration(duration)).foreachRDD(rdd => {
        // scalastyle:off
        println(s"count:${rdd.count()}")
        // scalastyle:on
        dstream.asInstanceOf[CanCommitOffsets].commitAsync()
      })
      ssc.checkpoint(checkpointDir)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, getStreamingContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def read(recordEntry: RecordEntry): String = {
    recordEntry.toJsonNode.toString
  }

}