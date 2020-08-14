package com.aliyun.spark.util

import java.io.File

import scala.xml.{Elem, Node, XML}
import java.net.InetAddress
import java.util.NoSuchElementException

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source
object GetConfForServerlessSpark {

  def getHostFile(silent: Boolean = true): Map[String, String] = {
    val hostsFiles = "/etc/hosts"
    val fileSource = Source.fromFile(hostsFiles)
    val hosts2Ip = fileSource
      .getLines()
      .map(_.trim)
      .filterNot(_.startsWith("#"))
      .map(_.split("\\s+"))
      .map(array => (array(0), array.takeRight(array.length - 1)))
      .flatMap(tuple => tuple._2.map((_, tuple._1)))
      .toMap
    fileSource.close()
    if (!silent) {
      hosts2Ip.foreach(println)
    }
    hosts2Ip
  }

  //Xpark-Spark 只能用yarn-cluster执行，而Xpack-Spark在slave节点上没有部署Hive，因此无法获取，这里我们硬编码
  def getHiveConf(_hiveConfDir: Option[String] = None,
                  useHosts: Boolean = true): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(false)
    }
    val hiveConfDir =
      _hiveConfDir.getOrElse(System.getenv("HIVE_CONF_DIR"))
    if (hiveConfDir == null) {
      throw new IllegalArgumentException(
        "HIVE_CONF_DIR 未找到，使用--hive-conf-dir 指定 HIVE_CONF_DIR"
      )
    }
    if (!new File(s"$hiveConfDir/hive-site.xml").exists()) {
      println(s"""
           |------------------您的hive-site.xml在slave机器：${InetAddress.getLocalHost.getHostName} 路径：${hiveConfDir}未找到----------------------
           |如果您使用的是Xpack-Spark集群中的Hive请使用以下配置：
           |spark.hadoop.hive.metastore.uris : "thrift://${hosts2Ips(
                   "spark-master1-1"
                 )}:9083,thrift://${hosts2Ips("spark-master2-1")}:9083"
           |""".stripMargin)
    } else {
      val hiveSite = XML.load(s"$hiveConfDir/hive-site.xml")

      try {
        val hiveMetaUris = getXmlValue(hiveSite, "hive.metastore.uris")
        println(
          "------------------为了在ServerlessSpark访问您的Hive，您应该配置下列参数----------------------"
        )
        val hiveMetaUriPattern =
          """thrift://([\d\w-\\.]+):?(\d+)?""".r("host", "port")
        val uriConfig = hiveMetaUris
          .split(",")
          .map(uri => {
            val hiveUri = hiveMetaUriPattern.findAllMatchIn(uri).next()
            val host = hiveUri.group("host")
            val port = hiveUri.group("port")
            val hostIp = getHostIp(host, hosts2Ips, useHosts)
            s"thrift://$hostIp:$port"
          })
        println(
          s"spark.hadoop.hive.metastore.uris : ${uriConfig.mkString(",")}"
        )
        println(
          "--------------------------------------end------------------------------------------------"
        )
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
  }

  def getHBaseConf(_hbaseConfDir: Option[String] = None,
                   useHosts: Boolean = true): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(false)
    }
    val hbaseConfDir =
      _hbaseConfDir.getOrElse(System.getenv("HBASE_CONF_DIR"))
    if (hbaseConfDir == null) {
      throw new IllegalArgumentException(
        "HBASE_CONF_DIR 未找到，使用--hbase-conf-dir 指定 HBASE_CONF_DIR"
      )
    }
    if (!new File(s"$hbaseConfDir/hbase-site.xml").exists()) {
      println(s"""
                 |------------------您的hbase-site.xml在slave机器：${InetAddress.getLocalHost.getHostName} 路径：${hbaseConfDir}未找到----------------------
                 |如果您使用的是云数据库Hbase版，可直接前往控制台->集群信息->数据库连接->ZK链接地址(专有网络)
                 |""".stripMargin)
    } else {
      val hbaseSite = XML.load(s"$hbaseConfDir/hbase-site.xml")

      try {
        val zookeeperHosts =
          getXmlValue(hbaseSite, "hbase.zookeeper.quorum").split(",")
        println(
          "------------------为了在ServerlessSpark访问您的Hbase，您应该给作业配置下列参数----------------------"
        )
        val zookeeperPattern =
          """([\d\w-\\.]+):?(\d+)?""".r("host", "port")

        val quorum = zookeeperHosts.map(elem => {
          val zookeeperHost = zookeeperPattern.findAllMatchIn(elem).next()
          val host = zookeeperHost.group("host")
          val port = zookeeperHost.group("port")
          val hostIp = getHostIp(host, hosts2Ips, useHosts)
          hostIp + (if (port == null) "" else ":" + port)
        }).mkString(",")
        println(
          s"您的hbase.zookeeper.quorum: $quorum"
        )
        println(
          "--------------------------------------end------------------------------------------------"
        )
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
  }

  def getHadoopConf(_hadoopConfDir: Option[String] = None,
                    useHosts: Boolean = true): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(false)
    }
    val hadoopConfDir =
      _hadoopConfDir.getOrElse(System.getenv("HADOOP_CONF_DIR"))
    if (hadoopConfDir == null) {
      throw new IllegalArgumentException(
        "HADOOP_CONF_DIR 未找到，使用--hadoop-conf-dir 指定 HADOOP_CONF_DIR"
      )
    }
    val coreSite = XML.loadFile(s"$hadoopConfDir/core-site.xml")
    val hdfsSite = XML.loadFile(s"$hadoopConfDir/hdfs-site.xml")
    try {
      val defaultFSValue = getXmlValue(coreSite, "fs.defaultFS")
      val defaultFSPattern = {
        """hdfs://([\d\w-\\.]+):?(\d+)?""".r
      }

      val defaultFSGrpups = defaultFSPattern
        .findAllMatchIn(defaultFSValue)
        .next()

      val hostname = defaultFSGrpups.group(1)
      val port = defaultFSGrpups.group(2)

      val ha = port == null

      if (ha) {
        println(
          "------------------your cluster is deployed in HA mode----------------------"
        )
        println(s"your defaultFS is: hdfs://$hostname")
        println("为了让SeverlessSpark访问您的Hadoop，您应该配置下列参数: ")

        val nameServicesValue = getXmlValue(hdfsSite, "dfs.nameservices")
        println(s"spark.hadoop.dfs.nameservices : $nameServicesValue")

        val proxyProvider = getXmlValue(
          hdfsSite,
          s"dfs.client.failover.proxy.provider.$nameServicesValue"
        )
        println(
          s"spark.hadoop.dfs.client.failover.proxy.provider.$nameServicesValue : ${proxyProvider}"
        )

        val nameNodeStr =
          getXmlValue(hdfsSite, s"dfs.ha.namenodes.$nameServicesValue")
        println(
          s"spark.hadoop.dfs.ha.namenodes.$nameServicesValue : ${nameNodeStr}"
        )

        val nameNodeList = nameNodeStr.split(",")
        val rpcPattern = { """([\d\w-\\.]+):(\d+)""".r }
        nameNodeList.foreach(nameNode => {
          val rpcAddress = getXmlValue(
            hdfsSite,
            s"dfs.namenode.rpc-address.$nameServicesValue.$nameNode"
          )
          val rpcRegex = rpcPattern.findAllMatchIn(rpcAddress).next()
          val rpcHost = rpcRegex.group(1)
          val rpcPort = rpcRegex.group(2)
          val rpcIp = getHostIp(rpcHost, hosts2Ips, useHosts)
          println(
            s"spark.hadoop.dfs.namenode.rpc-address.$nameServicesValue.$nameNode : $rpcIp:$rpcPort"
          )
        })
        println(
          "---------------------------------end----------------------------------------"
        )
      } else {
        println(
          "------------------your HDFS cluster is deployed in NOHA mode----------------------"
        )

        println(s"hostname: $hostname  port: $port")
        val addresses = InetAddress.getAllByName(hostname).head.getHostAddress
        println(s"your defaultFS is: hdfs://$addresses:$port")
        println(
          "---------------------------------end----------------------------------------"
        )
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }
  //需要使用 yarn-client 模式，在driver上执行
  def main(args: Array[String]): Unit = {
    //不要删除这句话，这句话用来启动一个跑在driver上的spark作业
    val sparkSession =
      SparkSession.builder().appName("GetSparkConf").getOrCreate()

    val parser = GetConfigCommandLine.parser
    val config = parser.parse(args, Config()) match {
      case Some(x) => x
      case None =>
        parser.showTryHelp
        throw new IllegalArgumentException()
    }
    for (target <- config.targets) {
      target match {
        case "hadoop" =>
          getHadoopConf(config.hadoopConfDir, useHosts = config.useHostsFile)
        case "hive" =>
          getHiveConf(config.hiveConfDir, useHosts = config.useHostsFile)
        case "hbase" =>
          getHBaseConf(config.hbaseConfDir, useHosts = config.useHostsFile)
        case _ =>
          throw new UnsupportedOperationException()
      }
    }
    sparkSession.stop()
  }

  def getXmlValue(node: Elem, name: String): String = {
    ((node \ "property")
      .find(elem => (elem \ "name").text.equals(name))
      .get \ "value").text
  }

  def getHostIp(hostName: String,
                hosts2Ip: Map[String, String],
                useHostsFile: Boolean): String = {
    try {
      if (useHostsFile) {
        hosts2Ip(hostName)
      } else {
        InetAddress.getByName(hostName).getHostAddress
      }
    } catch {
      case e: NoSuchElementException =>
        throw new NoSuchElementException(s"本地 Hosts文件中没有找到: $hostName")
    }
  }
}
