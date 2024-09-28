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
import com.github.drnushooz.spark.dynamodb.model.TestFruit
import org.apache.spark.sql.functions._

import scala.jdk.CollectionConverters._

class DefaultSourceTest extends TestBase {
  test("Table count is 9") {
    val count = spark.read.dynamodb(testTableName)
    count.show()
    assert(count.count() === 9)
  }

  test("Column sum is 27") {
    val result = spark.read.dynamodb(testTableName).collectAsList().asScala
    val numCols = result.map(_.length).sum
    assert(numCols === 27)
  }

  test("Select only first two columns") {
    val result =
      spark.read.dynamodb(testTableName).select("name", "color").collectAsList().asScala
    val numCols = result.map(_.length).sum
    assert(numCols === 18)
  }

  test("The least occurring color is yellow") {
    import spark.implicits._
    val itemWithLeastOccurringColor = spark.read
      .dynamodb(testTableName)
      .groupBy($"color")
      .agg(count($"color").as("countColor"))
      .orderBy($"countColor")
      .takeAsList(1)
      .get(0)
    assert(itemWithLeastOccurringColor.getAs[String]("color") === "yellow")
  }

  test("Test of attribute name alias") {
    import spark.implicits._
    val itemApple = spark.read
      .dynamodbAs[TestFruit](testTableName)
      .filter($"primaryKey" === "apple")
      .takeAsList(1)
      .get(0)
    assert(itemApple.primaryKey === "apple")
  }
}
