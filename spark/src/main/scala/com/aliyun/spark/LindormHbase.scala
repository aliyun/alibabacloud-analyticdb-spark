package com.aliyun.spark
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * To connect Lindorm Hbase, you need contact the DLA Spark support team to get corresponding connector jar.
 */
object LindormHbase {
  def main(args: Array[String]): Unit = {
    //HBase集群的ZK链接地址。//HBase集群的ZK链接地址。使用时请把此路径替换为你自己的HBase集群的zk访问地址。
    //格式为：xxx-002.hbase.rds.aliyuncs.com:2181,xxx-001.hbase.rds.aliyuncs.com:2181,xxx-003.hbase.rds.aliyuncs.com:2181
    val zkAddress = args(0)
    //hbase侧的表名，需要在hbase侧提前创建。hbase表创建可以参考：https://help.aliyun.com/document_detail/52051.html?spm=a2c4g.11174283.6.577.7e943c2eiYCq4k
    val hbaseTableName = args(1)
    //Spark侧的表名。
    val sparkTableName = args(2)

    val sparkSession = SparkSession
      .builder()
      //      .enableHiveSupport() //使用enableHiveSupport后通过spark jdbc查看到代码中创建的表
      .appName("scala spark on HBase test")
      .getOrCreate()

    //如果存在的话就删除Spark表
    sparkSession.sql(s"drop table if exists $sparkTableName")

    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", zkAddress)
    conf.set("hbase.client.username", "root")
    conf.set("hbase.client.password", "root")
    val namespace = "spark_test"

    //1. 定义Hbase表结构
    val tableName = TableName.valueOf(namespace, hbaseTableName)
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val cf = "cf"
    val column1 = cf + ":a"
    val column2 = cf + ":b"
    val htd = new HTableDescriptor(tableName)
    htd.addFamily(new HColumnDescriptor(cf))
    //2. 如果namespace不存在，则需要先创建namespace，再创建表
    //admin.createNamespace(NamespaceDescriptor.create(namespace).build())
    admin.createTable(htd)
    //3. 插入测试数据
    val rng = new Random()
    val k: Array[Byte] = new Array[Byte](3)
    val famAndQf1 = KeyValue.parseColumn(Bytes.toBytes(column1))
    val famAndQf2 = KeyValue.parseColumn(Bytes.toBytes(column2))
    val puts = new java.util.ArrayList[Put]()
    var i = 0
    for (b1 <- ('a' to 'z')) {
      for (b2 <- ('a' to 'z')) {
        for (b3 <- ('a' to 'z')) {
          if(i < 10) {
            k(0) = b1.toByte
            k(1) = b2.toByte
            k(2) = b3.toByte
            val put = new Put(k)
            put.addColumn(famAndQf1(0), famAndQf1(1), ("value_" + b1 + b2 + b3).getBytes())
            put.addColumn(famAndQf2(0), famAndQf2(1), ("value_" + b1 + b2 + b3).getBytes())
            puts.add(put)
            i = i + 1
          }
        }
      }
    }
    val table = conn.getTable(tableName)
    table.put(puts)
    //4. 查看Hbase数据
    val createCmd =
      s"""CREATE TABLE ${sparkTableName} USING org.apache.hadoop.hbase.spark
         |    OPTIONS ('catalog'=
         |    '{"table":{"namespace":"${namespace}", "name":"${hbaseTableName}"},
         |    "rowkey":"rowkey",
         |    "columns":{
         |    "col0":{"cf":"rowkey", "col":"rowkey", "type":"string"},
         |    "col1":{"cf":"cf", "col":"a", "type":"String"},
         |    "col2":{"cf":"cf", "col":"b", "type":"String"}}}',
         |    'hbase.zookeeper.quorum' = '${zkAddress}',
         |    'hbase.client.username'='root',
         |    'hbase.client.password'='root'
         |    )""".stripMargin

    println(s" the create sql cmd is: \n $createCmd")
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 10"
    val queryDF = sparkSession.sql(querySql)
    queryDF.show()

    //通过Spark写入数据到HBase
    val catalog =
      s"""
         |{
         |"table":{"namespace":"${namespace}", "name":"${hbaseTableName}"},
         |"rowkey":"rowkey",
         |"columns":{
         |    "col0":{"cf":"rowkey", "col":"rowkey", "type":"string"},
         |    "col1":{"cf":"cf", "col":"col1", "type":"String"},
         |    "col2":{"cf":"cf", "col":"b", "type":"String"}
         |    }
         |}
         |""".stripMargin

    queryDF.write.options(
      Map("catalog" -> catalog,
        "newtable" -> "3",
        "hbase.zookeeper.quorum" -> zkAddress,
        "hbase.client.username" -> "root",
        "hbase.client.password" -> "root")) // create 3 regions
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}

