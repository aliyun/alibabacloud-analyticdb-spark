package com.aliyun.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkCassandra {
  def main(args: Array[String]): Unit = {
    //cassandra的私网连接点
    val cHost = args(0)
    //Cassandra的CQL端口
    val cPort = args(1)
    //Cassandra的数据库用户名和密码
    val cUser = args(2)
    val cPw = args(3)
    //Cassandra的keystone和table
    val cKeySpace = args(4)
    val cTable = args(5)

    val spark = SparkSession
      .builder()
      .config("spark.cassandra.connection.host", cHost)
      .config("spark.cassandra.connection.port", cPort)
      .config("spark.cassandra.auth.username", cUser)
      .config("spark.cassandra.auth.password", cPw)
      .getOrCreate();

    val cData1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> cTable, "keyspace" -> cKeySpace))
      .load()
    print("=======start to print the cassandra data======")
    cData1.printSchema()
    cData1.show()
    import spark.implicits._
    //Write to Cassandra
    Seq(("james","brown"),("alice","xx")).toDF("first_name","last_name")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> cTable, "keyspace" -> cKeySpace))
      .mode(SaveMode.Append)
      .save()
    val cData2 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> cTable, "keyspace" -> cKeySpace))
      .load()
    cData2.show()
  }
}
