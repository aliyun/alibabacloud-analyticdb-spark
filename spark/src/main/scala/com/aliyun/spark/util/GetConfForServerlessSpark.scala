package com.aliyun.spark.util

import java.net.InetAddress

import CommonUtils._
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
        "HIVE_CONF_DIR not found, use --hive-conf-dir to specify HIVE_CONF_DIR\n"
      )
    }
    if (!checkDirectoryExist(hiveConfDir)) {
      throw new IllegalArgumentException(
        s"hive-conf-dir: $hiveConfDir does not exist or is not a directory"
      )
    }

    if (!checkFileExist(s"$hiveConfDir/hive-site.xml")) {
      println(s"""
           |------------------your hive-site.xml not found, Path: ${hiveConfDir} Machine: ${InetAddress.getLocalHost.getHostName}----------------------
           |If you are using hive in the xpack-spark cluster, use the following configuration：
           |spark.hadoop.hive.metastore.uris : "thrift://${hosts2Ips(
                   "spark-master1-1"
                 )}:9083,thrift://${hosts2Ips("spark-master2-1")}:9083"
           |Other cases can refer to user document<To be added>
           |""".stripMargin)
    } else {
      val hiveSite = loadXml(s"$hiveConfDir/hive-site.xml")
      val hiveMetaUris = getXmlValue(hiveSite, "hive.metastore.uris")
      println(
        """
            |--------------------------------------------------------------------------------------
            |To access your hive in serverless spark, you should configure the following parameters,
            |If your hive cluster is built on a highly available HDFS cluster,
            |you also need to configure the HDFS parameters
            |---------------------------------------------------------------------------------------""".stripMargin
      )
      val hiveMetaUriPattern =
        """thrift://([\d\w-\\.]+):?(\d+)?""".r("host", "port")
      val uriConfig = hiveMetaUris
        .split(",")
        .map(uri => {
          val hiveUris = hiveMetaUriPattern.findAllMatchIn(uri)
          if (!hiveUris.hasNext) {
            throw new IllegalArgumentException(
              s"""hive.metastore.uris: $uri mismatch `thrift://([\\d\\w-\\\\.]+):?(\\d+)?`"""
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
        "HBASE_CONF_DIR not found，use --hbase-conf-dir to specify HBASE_CONF_DIR"
      )
    }
    if (!checkDirectoryExist(hbaseConfDir)) {
      throw new IllegalArgumentException(
        s"hbase-conf-dir: $hbaseConfDir does not exist or is not a directory"
      )
    }
    if (!checkFileExist(s"$hbaseConfDir/hbase-site.xml")) {
      println(s"""
           |------------------your hbase-site.xml not found, Path: ${hbaseConfDir}  Machine: ${InetAddress.getLocalHost.getHostName}----------------------
           |If you are using the cloud database HBase version,
           |you can directly go to the
           |`console -> cluster information -> database connection -> ZK link address (VPC)`
           |to view the corresponding information
           |Other cases can refer to user document<To be added>
           |""".stripMargin)
    } else {
      val hbaseSite = loadXml(s"$hbaseConfDir/hbase-site.xml")
      val zookeeperHosts =
        getXmlValue(hbaseSite, "hbase.zookeeper.quorum").split(",")
      println(
        "------------------To access your Hbase in serverless spark, you should configure the following parameters----------------------"
      )
      val zookeeperPattern =
        """([\d\w-\\.]+):?(\d+)?""".r("host", "port")
      val quorum = zookeeperHosts
        .map(elem => {
          val zookeeperHosts = zookeeperPattern.findAllMatchIn(elem)
          if (!zookeeperHosts.hasNext) {
            throw new IllegalArgumentException(
              s"""hbase.zookeeper.quorum: $elem mismatch `([\\d\\w-\\\\.]+):?(\\d+)?`"""
            )
          }
          val zookeeperHost = zookeeperHosts.next
          val host = zookeeperHost.group("host")
          val port = zookeeperHost.group("port")
          val hostIp = getHostIp(host, hosts2Ips, useHosts)
          hostIp + (if (StringUtils.isBlank(port)) "" else ":" + port)
        })
        .mkString(",")
      println(s"your hbase.zookeeper.quorum: $quorum")
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
        "HADOOP_CONF_DIR not found，use --hadoop-conf-dir to specify HADOOP_CONF_DIR"
      )
    }
    if (!checkDirectoryExist(hadoopConfDir)) {
      throw new IllegalArgumentException(
        s"hadoop-conf-dir: $hadoopConfDir does not exist or is not a directory"
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
        s"""defaultFS: $defaultFSValue  mismatch `hdfs://([\\d\\w-\\\\.]+):?(\\d+)?`"""
      )
    }
    val defaultFSGroup = defaultFSGrpups.next()
    val hostname = defaultFSGroup.group(1)
    val port = defaultFSGroup.group(2)
    val ha = port == null

    if (ha) {
      println("------------------Your HDFS cluster is deployed in high availability mode----------------------")
      println(s"your defaultFS is: hdfs://$hostname")
      println("To access your HDFS in serverless spark, you should configure the following parameters")

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
        .getOrCreate()

    val parser = GetConfigCommandLine.parser
    val config = parser.parse(args, Config()) match {
      case Some(x) => x
      case None =>
        parser.showTryHelp
        throw new IllegalArgumentException("Unable to parse parameter correctly, wrong parameter input")
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
