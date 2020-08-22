package com.aliyun.spark.util

import org.apache.hadoop.util.hash.Hash

import scala.collection.mutable

case class Config(hbaseConfDir: Option[String] = None,
                  hadoopConfDir: Option[String] = None,
                  hiveConfDir: Option[String] = None,
                  hostsFilePath: Option[String] = None,
                  targets: Seq[String] = Seq.empty[String],
                  mode: String = "",
                  useHostsFile: Boolean = true,
                  verbose: Boolean = false)

object GetConfigCommandLine {
  val supportComponent = mutable.HashSet("hadoop", "hive", "hbase")

  val parser = new scopt.OptionParser[Config]("GetConfForServerlessSpark") {
    head("GetConfigCommandLine", "0.0.1-SNAPSHOT")
    note("GetSparkConf get hadoop")
    note("GetSparkConf get hadoop ")
    note(
      "GetSparkConf get --hadoop-conf-dir <path/to/hadoop/conf/dir> hadoop hive\n"
    )
    opt[String]("hadoop-conf-dir")
      .valueName("<path/to/hadoop/conf/dir>")
      .action((x, c) => c.copy(hadoopConfDir = Some(x)))
      .optional()
      .maxOccurs(1)
      .text("指定hadoop conf dir路径, 默认会从集群环境变量 HADOOP_CONF_DIR 读取")
    opt[String]("hive-conf-dir")
      .valueName("<path/to/hive/conf/dir>")
      .action((x, c) => c.copy(hiveConfDir = Some(x)))
      .optional()
      .maxOccurs(1)
      .text("指定hive conf dir路径, 默认会从集群环境变量 HIVE_CONF_DIR 读取")
    opt[String]("hosts-file-path")
      .valueName("<path/to/hosts/file>")
      .action((x, c) => c.copy(hostsFilePath = Some(x)))
      .optional()
      .hidden()
      .maxOccurs(1)
      .text("指定hostsfile的路径，用于本地测试，默认隐藏选项")
    opt[Boolean]("use-hosts-file")
      .hidden()
      .valueName("true|false")
      .action((x, c) => c.copy(useHostsFile = x))
      .optional()
      .text("true 使用本地/etc/hosts 解析域名， false 使用Java InetAddress 解析域名, 默认为true")
    opt[Unit]("verbose")
      .action((_, c) => c.copy(verbose = false))
      .optional()
      .text("指定--verbose输出更多调试信息")
    opt[String]("hbase-conf-dir")
      .valueName("<path/to/hbase/conf/dir>")
      .action((x, c) => c.copy(hiveConfDir = Some(x)))
      .optional()
      .maxOccurs(1)
      .text("指定hbae conf dir路径,默认会从集群环境变量 HIVE_CONF_DIR 读取")
    help("help").text("prints this usage text")
    cmd("get")
      .action((x, c) => c.copy(mode = "get"))
      .required()
      .children(
        arg[String]("<hadoop|hive|hbase|>")
          .unbounded()
          .required()
          .action((x, c) => c.copy(targets = c.targets :+ x))
          .text("get hadoop|hive|hbase，可填写多个，以空格分开"),
        checkConfig(c => {
          val illegalTargets = c.targets.filterNot(supportComponent.contains)
          if (illegalTargets.nonEmpty) {
            failure(s"不支持获取这些组件的参数：${illegalTargets}")
          } else {
            success
          }
        })
      )
    checkConfig(c => {
      if (!c.mode.equals("get")) {
        showUsage
        failure("使用GetSparkConf 需要输入 `get` 命令 ")
      } else {
        success
      }
    })

  }

  def main(args: Array[String]): Unit = {
    val config = parser.parse(args, Config()) match {
      case Some(x) => x
      case None =>
        parser.showTryHelp
        throw new IllegalArgumentException()
    }
    println(config.targets)
    println(config.hadoopConfDir)
  }
}
