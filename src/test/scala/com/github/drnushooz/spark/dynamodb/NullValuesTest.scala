/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.drnushooz.spark.dynamodb

import com.github.drnushooz.spark.dynamodb.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class NullValuesTest extends TestBase {
  test("Insert nested StructType with null values") {
    val testTableName = "StructNullValues"
    createTestTable(testTableName)

    val schema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField(
          "info",
          StructType(
            Seq(StructField("age", IntegerType, nullable = true), StructField("address", StringType, nullable = true))),
          nullable = true)))

    val rows = spark.sparkContext.parallelize(
      Seq(Row("one", Row(30, "Somewhere")), Row("two", null), Row("three", Row(null, null))))

    val newItemsDs = spark.createDataFrame(rows, schema)
    newItemsDs.write.dynamodb(testTableName)

    val validationDs = spark.read.dynamodb(testTableName).select("info.*")
    assert(validationDs.schema.fieldNames.isEmpty)
  }
}
