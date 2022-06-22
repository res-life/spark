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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class WriteParquetWithPartitionsTest extends QueryTest with SharedSparkSession {
  test("write Parquet with partitions") {
    val data = spark.sparkContext.parallelize(
      Seq(Row("r1", 1), Row("r2", 2), Row("r3", 3), Row("r4", 4), Row("r5", 5), Row("r6", 6),
        Row("r1", 1), Row("r2", 2), Row("r3", 3), Row("r4", 4), Row("r5", 5), Row("r6", 6)), 2)
    val schema = StructType(Array(StructField("c1", StringType), StructField("c2", IntegerType)))
    val df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").partitionBy("c1").parquet("/tmp/my-p")
  }
}
