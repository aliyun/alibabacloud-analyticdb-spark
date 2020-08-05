/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.spark

import org.apache.spark.sql.SparkSession

object SparkRDS {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("rds test")
      .getOrCreate()

    val url = args(0)
    val dbtable = args(1)
    val user = args(2)
    val password = args(3)

    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .load()

    jdbcDF.show()
  }

}
