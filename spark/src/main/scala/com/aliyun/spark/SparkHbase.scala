package com.aliyun.spark

import org.apache.spark.sql.SparkSession

object SparkHbase {
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

    //如果存在的话就删除表
    sparkSession.sql(s"drop table if exists $sparkTableName")

    val createCmd =
      s"""CREATE TABLE ${sparkTableName} USING org.apache.hadoop.hbase.spark
         |    OPTIONS ('catalog'=
         |    '{"table":{"namespace":"default", "name":"${hbaseTableName}"},"rowkey":"rowkey",
         |    "columns":{
         |    "col0":{"cf":"rowkey", "col":"rowkey", "type":"string"},
         |    "col1":{"cf":"cf", "col":"col1", "type":"String"}}}',
         |    'hbase.zookeeper.quorum' = '${zkAddress}'
         |    )""".stripMargin

    println(s" the create sql cmd is: \n $createCmd")
    sparkSession.sql(createCmd)
    val querySql = "select * from " + sparkTableName + " limit 10"
    val queryDF = sparkSession.sql(querySql)
    queryDF.show()


    val catalog =
      s"""
        |{
        |"table":{"namespace":"default", "name":"${hbaseTableName}"},
        |"rowkey":"rowkey",
        |"columns":{
        |    "col0":{"cf":"rowkey", "col":"rowkey", "type":"string"},
        |    "col1":{"cf":"cf", "col":"col1", "type":"String"}
        |    }
        |}
        |""".stripMargin
    queryDF.write.options(
      Map("catalog" -> catalog, "newtable" -> "3","hbase.zookeeper.quorum"->zkAddress)) // create 3 regions
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}
