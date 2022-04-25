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

package org.apache.spark.my

import java.sql.Date

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MyDebug extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("to_timestamp") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val data = spark.sparkContext.parallelize(
        Seq(Row("2019-03-01 10:11:12"), Row("2019-03-01 10:11:12")))
      val df = spark.createDataFrame(
        data,
        StructType(Array(StructField("t", StringType))))
      df.createOrReplaceTempView("tab")

      val str = spark.sql("select to_timestamp(t, 'yyyy-MM-dd HH:mm:ss') from tab").explain()
      print(str)
    }
  }

  test("date add interval") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true",
      //      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "true") {
      val data = spark.sparkContext.parallelize(
        Seq(Row(new Date(1970, 1, 1)), Row(new Date(1970, 1, 2))))
      val df = spark.createDataFrame(
        data,
        StructType(Array(StructField("c1", DateType))))
      df.createOrReplaceTempView("tab")

      val ret = spark.sql("select c1 + (interval 3 days 5 seconds) as x from tab").collect()
      print(ret)
    }
  }

  test("cast string to date") {
    var d1: Option[Int] = Option.empty

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970 "))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01 A"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01  A"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01T"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01TA"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01T A"))
    assert(d1.nonEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970A"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970 A"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970T"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970 T"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01T"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01 A"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("1970-01-01A"))
    assert(d1.isEmpty)

    d1 = DateTimeUtils.stringToDate(UTF8String.fromString("2022-02-29"))
    assert(d1.isEmpty)
  }

  test("my-tmp") {
  }

  test("map_filter") {
    val dfInts = Seq(
      Map("a" -> "a", "b" -> "b"),
      Map("aa" -> "aa", "bb" -> "bb")).toDF("m")

    checkAnswer(dfInts.selectExpr(
      "map_filter(m, (k, v) -> k === v)"),
      Seq(
        Row(Map("a" -> "a", "b" -> "b")),
        Row(Map("a" -> "a", "b" -> "b"))))
  }
}
