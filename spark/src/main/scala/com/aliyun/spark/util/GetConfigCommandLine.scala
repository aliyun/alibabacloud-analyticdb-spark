package com.aliyun.spark.util

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
      .text("Specify the hadoop-conf-dir path. By default will read from the cluster environment variable HADOOP_CONF_DIR")
    opt[String]("hive-conf-dir")
      .valueName("<path/to/hive/conf/dir>")
      .action((x, c) => c.copy(hiveConfDir = Some(x)))
      .optional()
      .maxOccurs(1)
      .text("Specify the hive-conf-dir path. By default will read from the cluster environment variable HIVE_CONF_DIR")
    opt[String]("hosts-file-path")
      .valueName("<path/to/hosts/file>")
      .action((x, c) => c.copy(hostsFilePath = Some(x)))
      .optional()
      .hidden()
      .maxOccurs(1)
      .text("Specify the path of hostsfile, which is used for local test, and is hidden by default")
    opt[Boolean]("use-hosts-file")
      .hidden()
      .valueName("true|false")
      .action((x, c) => c.copy(useHostsFile = x))
      .optional()
      .text("true: uses the local /etc/hosts to resolve the domain name," +
        "false: uses Java InetAddress to resolve the domain name. The default value is true")
    opt[Unit]("verbose")
      .action((_, c) => c.copy(verbose = false))
      .optional()
      .text("output more debugging information")
    opt[String]("hbase-conf-dir")
      .valueName("<path/to/hbase/conf/dir>")
      .action((x, c) => c.copy(hiveConfDir = Some(x)))
      .optional()
      .maxOccurs(1)
      .text("Specify the hbase-conf-dir path. By default will read from the cluster environment variable HBASE_CONF_DIR")
    help("help").text("prints this usage text")
    cmd("get")
      .action((x, c) => c.copy(mode = "get"))
      .required()
      .children(
        arg[String]("<hadoop|hive|hbase|>")
          .unbounded()
          .required()
          .action((x, c) => c.copy(targets = c.targets :+ x))
          .text("get hadoop|hive|hbase, More than one can be filled in, separated by spaces"),
        checkConfig(c => {
          val illegalTargets = c.targets.filterNot(supportComponent.contains)
          if (illegalTargets.nonEmpty) {
            failure(s"these components is not supported：${illegalTargets}")
          } else {
            success
          }
        })
      )
    checkConfig(c => {
      if (!c.mode.equals("get")) {
        showUsage
        failure("required GetSparkConf `get` command")
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
