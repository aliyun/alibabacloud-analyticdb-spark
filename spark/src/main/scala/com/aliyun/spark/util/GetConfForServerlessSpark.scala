package com.aliyun.spark.util

import scala.xml.{Elem, Node, XML}
import java.net.InetAddress

import java.util.NoSuchElementException
import CommonUtils._
import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
object GetConfForServerlessSpark {

  //Xpark-Spark 只能用yarn-cluster执行，而Xpack-Spark在slave节点上没有部署Hive，因此无法获取，这里我们硬编码
  def getHiveConf(_hiveConfDir: Option[String] = None,
                  useHosts: Boolean = true,
                  hostsFilePath: Option[String] = None,
                  verbose: Boolean = false): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(silent = !verbose, hostsFilePath)
    }
    val hiveConfDir =
      _hiveConfDir.getOrElse(System.getenv("HIVE_CONF_DIR"))

    if (hiveConfDir == null) {
      throw new IllegalArgumentException(
        "HIVE_CONF_DIR 未找到，使用--hive-conf-dir 指定 HIVE_CONF_DIR"
      )
    }
    if (!checkDirectoryExist(hiveConfDir)) {
      throw new IllegalArgumentException(
        s"hive-conf-dir: $hiveConfDir 不存在或不是一个目录"
      )
    }

    if (!checkFileExist(s"$hiveConfDir/hive-site.xml")) {
      println(s"""
           |------------------您的hive-site.xml在slave机器：${InetAddress.getLocalHost.getHostName} 路径：${hiveConfDir}未找到----------------------
           |如果您使用的是Xpack-Spark集群中的Hive请使用以下配置：
           |spark.hadoop.hive.metastore.uris : "thrift://${hosts2Ips(
                   "spark-master1-1"
                 )}:9083,thrift://${hosts2Ips("spark-master2-1")}:9083"
           |如果您是自建Hive集群用户，请在$${HADOOP_CONF_IDR}/hive-site.xml中查找hive.meta.uris中的值，并将域名替换为对应的ip
           |如果您的Hive简历在Hadoop HA的集群上，您还需要额外配置Hadoop参数，具体可以参照用户文档<用户文档url>
           |""".stripMargin)
    } else {
      val hiveSite = loadXml(s"$hiveConfDir/hive-site.xml")
      try {
        val hiveMetaUris = getXmlValue(hiveSite, "hive.metastore.uris")
        println("""
            |------------------为了在ServerlessSpark访问您的Hive，您应该配置下列参数,
            |如果您的HIVE集群是建立在高用的HDFS集群上您还需要配置HDFS参数--------------""".stripMargin)
        val hiveMetaUriPattern =
          """thrift://([\d\w-\\.]+):?(\d+)?""".r("host", "port")
        val uriConfig = hiveMetaUris
          .split(",")
          .map(uri => {
            val hiveUris = hiveMetaUriPattern.findAllMatchIn(uri)
            if (!hiveUris.hasNext) {
              throw new IllegalArgumentException(
                s"""hive.metastore.uris的值: $uri 格式不符合`thrift://([\\d\\w-\\\\.]+):?(\\d+)?`规范"""
              )
            }
            val hiveUri = hiveUris.next()
            val host = hiveUri.group("host")
            val port = hiveUri.group("port")
            val hostIp = getHostIp(host, hosts2Ips, useHosts)
            "thrift://" + hostIp + (if (StringUtils.isBlank(port)) ""
                                    else ":" + port)
          })
        println(s"""
             |"spark.hadoop.hive.metastore.uris" : \"${uriConfig.mkString(",")}"
             |""".stripMargin)
        println(
          "--------------------------------------end------------------------------------------------\n"
        )
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
  }

  //TODO:确认访问HBASE是否需要配置HADOOP参数
  def getHBaseConf(_hbaseConfDir: Option[String] = None,
                   useHosts: Boolean = true,
                   hostsFilePath: Option[String] = None,
                   verbose: Boolean = false): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(silent = !verbose, hostsFilePath)
    }
    val hbaseConfDir =
      _hbaseConfDir.getOrElse(System.getenv("HBASE_CONF_DIR"))
    if (hbaseConfDir == null) {
      throw new IllegalArgumentException(
        "HBASE_CONF_DIR 未找到，使用--hbase-conf-dir 指定 HBASE_CONF_DIR"
      )
    }
    if (!checkDirectoryExist(hbaseConfDir)) {
      throw new IllegalArgumentException(
        s"hbase-conf-dir: $hbaseConfDir 不存在或不是一个目录"
      )
    }
    if (!checkFileExist(s"$hbaseConfDir/hbase-site.xml")) {
      println(s"""
           |------------------您的hbase-site.xml在slave机器：${InetAddress.getLocalHost.getHostName} 路径：${hbaseConfDir}未找到----------------------
           |如果您使用的是云数据库Hbase版，可直接前往控制台->集群信息->数据库连接->ZK链接地址(专有网络),查看对应信息
           |如果您的Hbase是自建集群，请参考用户文档<用户文档url>
           |""".stripMargin)
    } else {
      val hbaseSite = loadXml(s"$hbaseConfDir/hbase-site.xml")
      val zookeeperHosts =
        getXmlValue(hbaseSite, "hbase.zookeeper.quorum").split(",")
      println(
        "------------------为了在ServerlessSpark访问您的Hbase，您应该给作业配置下列参数----------------------"
      )
      val zookeeperPattern =
        """([\d\w-\\.]+):?(\d+)?""".r("host", "port")
      val quorum = zookeeperHosts
        .map(elem => {
          val zookeeperHosts = zookeeperPattern.findAllMatchIn(elem)
          if (!zookeeperHosts.hasNext) {
            throw new IllegalArgumentException(
              s"""hbase.zookeeper.quorum的值: $elem 格式不符合`([\\d\\w-\\\\.]+):?(\\d+)?`规范"""
            )
          }
          val zookeeperHost = zookeeperHosts.next
          val host = zookeeperHost.group("host")
          val port = zookeeperHost.group("port")
          val hostIp = getHostIp(host, hosts2Ips, useHosts)
          hostIp + (if (StringUtils.isBlank(port)) "" else ":" + port)
        })
        .mkString(",")
      println(s"您的hbase.zookeeper.quorum: $quorum")
      println(
        "--------------------------------------end------------------------------------------------\n"
      )
    }
  }

  def getHadoopConf(_hadoopConfDir: Option[String] = None,
                    useHosts: Boolean = true,
                    hostsFilePath: Option[String] = None,
                    verbose: Boolean = false): Unit = {
    var hosts2Ips: Map[String, String] = Map.empty[String, String]
    if (useHosts) {
      hosts2Ips = getHostFile(silent = !verbose, hostsFilePath)
    }
    val hadoopConfDir =
      _hadoopConfDir.getOrElse(System.getenv("HADOOP_CONF_DIR"))
    if (hadoopConfDir == null) {
      throw new IllegalArgumentException(
        "HADOOP_CONF_DIR 未找到，使用--hadoop-conf-dir 指定 HADOOP_CONF_DIR"
      )
    }
    if (!checkDirectoryExist(hadoopConfDir)) {
      throw new IllegalArgumentException(
        s"hadoop-conf-dir: $hadoopConfDir 不存在或不是一个目录"
      )
    }
    val coreSite = loadXml(s"$hadoopConfDir/core-site.xml")
    val hdfsSite = loadXml(s"$hadoopConfDir/hdfs-site.xml")
    val defaultFSValue = getXmlValue(coreSite, "fs.defaultFS")
    val defaultFSPattern = {
      """hdfs://([\d\w-\\.]+):?(\d+)?""".r
    }
    val defaultFSGrpups = defaultFSPattern
      .findAllMatchIn(defaultFSValue)
    if (!defaultFSGrpups.hasNext) {
      throw new IllegalArgumentException(
        s"""defaultFS的值: $defaultFSValue 格式不符合`hdfs://([\\d\\w-\\\\.]+):?(\\d+)?`规范"""
      )
    }
    val defaultFSGroup = defaultFSGrpups.next()
    val hostname = defaultFSGroup.group(1)
    val port = defaultFSGroup.group(2)
    val ha = port == null

    if (ha) {
      println("------------------您的HDFS集群以高可用模式部署----------------------")
      println(s"your defaultFS is: hdfs://$hostname")
      println("为了让SeverlessSpark访问您的Hadoop，您应该配置下列参数: ")

      val nameServicesValue = getXmlValue(hdfsSite, "dfs.nameservices")
      println(s"""
           |"spark.hadoop.dfs.nameservices" : "$nameServicesValue"
           |""".stripMargin)

      val proxyProvider = getXmlValue(
        hdfsSite,
        s"dfs.client.failover.proxy.provider.$nameServicesValue"
      )
      println(s"""
           |"spark.hadoop.dfs.client.failover.proxy.provider.$nameServicesValue\" : \"${proxyProvider}"
           |""".stripMargin)

      val nameNodesValuesStr =
        getXmlValue(hdfsSite, s"dfs.ha.namenodes.$nameServicesValue")
      println(s"""
           |"spark.hadoop.dfs.ha.namenodes.$nameServicesValue" : "${nameNodesValuesStr}"
           |""".stripMargin)

      val allNameNodesValues = nameNodesValuesStr.split(",")
      val rpcPattern = {
        """([\d\w-\\.]+):(\d+)""".r
      }
      allNameNodesValues.foreach(nameNodesValue => {
        val rpcAddressStr = getXmlValue(
          hdfsSite,
          s"dfs.namenode.rpc-address.$nameServicesValue.$nameNodesValue"
        )
        val rpcAddressValues = rpcPattern.findAllMatchIn(rpcAddressStr)
        if (!rpcAddressValues.hasNext) {
          throw new IllegalArgumentException(
            s""" dfs.namenode.rpc-address.$nameServicesValue.$nameNodesValue: $rpcAddressStr 格式不符合(\\d\\w-\\\\.]+):(\\d+)`规范"""
          )
        }
        val rpcAddressValue = rpcAddressValues.next()
        val rpcAddressHost = rpcAddressValue.group(1)
        val rpcAddressPort = rpcAddressValue.group(2)
        val rpcHostIp = getHostIp(rpcAddressHost, hosts2Ips, useHosts)
        println(s"""
             |"spark.hadoop.dfs.namenode.rpc-address.$nameServicesValue.$nameNodesValue\" : \"$rpcHostIp:$rpcAddressPort"
             |""".stripMargin)
      })
      println(
        "---------------------------------end----------------------------------------\n"
      )
    } else {
      println(
        "------------------your HDFS cluster is deployed in NOHA mode----------------------"
      )
      val hostIp = getHostIp(hostname, hosts2Ips, useHosts)
      println(s"your defaultFS is: hdfs://$hostIp:$port")
      println(
        "---------------------------------end----------------------------------------\n"
      )
    }
  }

  //需要使用 yarn-client 模式，在driver上执行
  def main(args: Array[String]): Unit = {
//    不要删除这句话，这句话用来启动一个跑在driver上的spark作业
    val sparkSession =
      SparkSession
        .builder()
        .appName("GetSparkConf")
        .master("local")
        .getOrCreate()

    val parser = GetConfigCommandLine.parser
    val config = parser.parse(args, Config()) match {
      case Some(x) => x
      case None =>
        parser.showTryHelp
        throw new IllegalArgumentException("无法正确解析参数，参数输入有误")
    }
    for (target <- config.targets) {
      target match {
        case "hadoop" =>
          getHadoopConf(
            config.hadoopConfDir,
            useHosts = config.useHostsFile,
            hostsFilePath = config.hostsFilePath,
            verbose = config.verbose
          )
        case "hive" =>
          getHiveConf(
            config.hiveConfDir,
            useHosts = config.useHostsFile,
            hostsFilePath = config.hostsFilePath,
            verbose = config.verbose
          )
        case "hbase" =>
          getHBaseConf(
            config.hbaseConfDir,
            useHosts = config.useHostsFile,
            hostsFilePath = config.hostsFilePath,
            verbose = config.verbose
          )
        case _ =>
          throw new UnsupportedOperationException()
      }
    }
    sparkSession.stop()
  }

}
