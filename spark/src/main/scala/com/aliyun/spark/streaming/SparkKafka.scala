package com.aliyun.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkKafka {
  def main(args: Array[String]): Unit = {
    if(args.length < 3){
      System.err.println(
        """
          |args0: groupId
          |args1: topicName
          |args2: bootstrapServers
          |""".stripMargin)
      System.exit(1)
    }
    val groupId = args(0)
    val topicName = args(1)
    val bootstrapServers = args(2)


    val sparkConf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("SparkKafkaSub")
    sparkConf.registerKryoClasses(Array(classOf[ConsumerRecord[_,_]]))

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("group.id", groupId)
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()

  }
}
