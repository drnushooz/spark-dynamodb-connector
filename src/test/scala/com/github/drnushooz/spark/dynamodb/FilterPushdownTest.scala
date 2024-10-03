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

class FilterPushdownTest extends TestBase {
  test("Count of red fruit is 2 (`EqualTo` filter)") {
    import spark.implicits._
    val fruitCount = spark.read.dynamodb(testTableName).where($"color" === "red").count()
    assert(fruitCount === 2)
  }

  test("Count of yellow and green fruit is 4 (`In` filter)") {
    import spark.implicits._
    val fruitCount = spark.read
      .dynamodb(testTableName)
      .where($"color" isin ("yellow", "green"))
      .count()
    assert(fruitCount === 4)
  }

  test("Count of 0.01 weight fruit is 4 (`In` filter)") {
    import spark.implicits._
    val fruitCount = spark.read
      .dynamodb(testTableName)
      .where($"weightKg" isin 0.01)
      .count()
    assert(fruitCount === 3)
  }

  test("Only 'banana' starts with a 'b' and is >0.01 kg (`StringStartsWith`, `GreaterThan`, `And` filters)") {
    import spark.implicits._
    val fruit = spark.read
      .dynamodb(testTableName)
      .where(($"name" startsWith "b") && ($"weightKg" > 0.01))
      .collectAsList()
      .get(0)
    assert(fruit.getAs[String]("name") === "banana")
  }
}
