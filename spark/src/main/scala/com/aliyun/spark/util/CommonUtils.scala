package com.aliyun.spark.util

import java.io.{File, FileNotFoundException}
import java.net.InetAddress
import java.util.NoSuchElementException

import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils

import scala.io.Source
import scala.xml.{Elem, XML}

object CommonUtils {
  def getHostFile(silent: Boolean = true,
                  hostsFilePath: Option[String] = None): Map[String, String] = {
    val hostsFiles = hostsFilePath.getOrElse("/etc/hosts")
    val ipPattern = """^((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)$"""
    checkFileExist(hostsFiles)
    val fileSource = Source.fromFile(hostsFiles)
    val (fileSource_1,fileSource_2) = fileSource.getLines().duplicate
    val hosts2Ip = fileSource_1
      .map(_.trim)
      .filterNot(line => line.startsWith("#"))
      .filterNot(StringUtils.isBlank(_))
      .map(_.split("\\s+"))
      .filter(_(0).matches(ipPattern))
      .map(array => (array(0), array.takeRight(array.length - 1)))
      .flatMap(tuple => tuple._2.map((_, tuple._1)))
      .toMap

    if (!silent) {
      println("==============your hosts file contents==============")
      val anotherFileSource = fileSource_2
        .map(_.trim)
        .filterNot(line => line.startsWith("#"))
        .filterNot(StringUtils.isBlank(_))
        .filterNot(_.startsWith("#"))
        .mkString(",\n")
      println(anotherFileSource)
      println("=======================end==========================")
    }
    fileSource.close()
    if (hosts2Ip.isEmpty) {
      throw new IllegalArgumentException("hosts file is empty，can not resolve hostsname from hosts file")
    }
    hosts2Ip
  }

  def checkFileExistOrElseThrowException(fileName: String): Boolean = {
    if (!new File(fileName).exists()) {
      throw new FileNotFoundException(s"$fileName is not exist")
    } else {
      true
    }
  }

  def checkFileExist(fileName: String): Boolean = {
    new File(fileName).exists()
  }

  def checkDirectoryExist(dirName: String): Boolean = {
    new File(dirName).isDirectory
  }

  def checkKeyValueNotEmptyOrElseThrowException(key: String,
                                                str: String): Boolean = {
    if (StringUtils.isBlank(str)) {
      throw new IllegalArgumentException(s"key: $key is null or empty")
    } else {
      true
    }
  }

  def loadXml(fileName: String): Elem = {
    checkFileExistOrElseThrowException(fileName)
    val xml = XML.load(fileName)
    xml
  }

  def getXmlValue(node: Elem, name: String): String = {
    val allProperties = node \ "property"

    val targetProperty =
      allProperties.find(elem => (elem \ "name").text.equals(name))

    val targetValue = targetProperty.map(_ \ "value")

    val ans = targetValue
      .getOrElse(throw new IllegalArgumentException(s"key: $name not found"))
      .text
    checkKeyValueNotEmptyOrElseThrowException(name, ans)
    ans
  }

  def getHostIp(hostName: String,
                hosts2Ip: Map[String, String],
                useHostsFile: Boolean): String = {

    Preconditions.checkNotNull(hostName)
    if (useHostsFile) {
      Preconditions.checkNotNull(hosts2Ip)
      Preconditions.checkArgument(hosts2Ip.nonEmpty)
      hosts2Ip.getOrElse(
        hostName,
        throw new NoSuchElementException(s"not found in local hots file: $hostName, can not resolve $hostName")
      )
    } else {
      InetAddress.getByName(hostName).getHostAddress
    }
  }

}
