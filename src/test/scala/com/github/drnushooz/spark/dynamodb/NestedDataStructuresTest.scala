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
import com.github.drnushooz.spark.dynamodb.model.{TestFruitProperties, TestFruitWithProperties}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._

class NestedDataStructuresTest extends TestBase {
  test("Insert ArrayType") {
    import spark.implicits._

    val testTableName = "InsertTestList"
    createTestTable(testTableName)
    val fruitSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField("color", StringType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("properties", ArrayType(StringType, containsNull = false), nullable = false)))

    val rows = spark.sparkContext.parallelize(
      Seq(
        Row("lemon", "yellow", 0.1, Seq("fresh", "2 dkk")),
        Row("orange", "orange", 0.2, Seq("too ripe", "1 dkk")),
        Row("pomegranate", "red", 0.2, Seq("freshness", "4 dkk"))))

    val newItemsDs = spark.createDataFrame(rows, fruitSchema)

    newItemsDs.printSchema()
    newItemsDs.show(false)

    newItemsDs.write.dynamodb(testTableName)

    println("Writing successful.")

    val validationDs = spark.read.dynamodb(testTableName)
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select($"properties".as[Seq[String]])
        .collect()
        .forall(Seq(Seq("fresh", "2 dkk"), Seq("too ripe", "1 dkk"), Seq("freshness", "4 dkk")) contains _))
  }

  test("Insert MapType") {
    import spark.implicits._

    val testTableName = "InsertTestMap"
    createTestTable(testTableName)
    val fruitSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField("color", StringType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("properties", MapType(StringType, StringType, valueContainsNull = false))))

    val rows = spark.sparkContext.parallelize(
      Seq(
        Row("lemon", "yellow", 0.1, Map("freshness" -> "fresh", "eco" -> "yes", "price" -> "2 dkk")),
        Row("orange", "orange", 0.2, Map("freshness" -> "too ripe", "eco" -> "no", "price" -> "1 dkk")),
        Row("pomegranate", "red", 0.2, Map("freshness" -> "green", "eco" -> "yes", "price" -> "4 dkk"))))

    val newItemsDs = spark.createDataFrame(rows, fruitSchema)

    newItemsDs.printSchema()
    newItemsDs.show(false)

    newItemsDs.write.dynamodb(testTableName)

    println("Writing successful.")

    val validationDs = spark.read.schema(fruitSchema).dynamodb(testTableName)
    validationDs.show(false)
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select($"properties".as[Map[String, String]])
        .collect()
        .forall(Seq(
          Map("freshness" -> "fresh", "eco" -> "yes", "price" -> "2 dkk"),
          Map("freshness" -> "too ripe", "eco" -> "no", "price" -> "1 dkk"),
          Map("freshness" -> "green", "eco" -> "yes", "price" -> "4 dkk")) contains _))
  }

  test("Insert ArrayType with nested MapType") {
    import spark.implicits._

    val testTableName = "InsertTestListMap"
    createTestTable(testTableName)
    val fruitSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField("color", StringType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField(
          "properties",
          ArrayType(MapType(StringType, StringType, valueContainsNull = false), containsNull = false),
          nullable = false)))

    val rows = spark.sparkContext.parallelize(
      Seq(
        Row("lemon", "yellow", 0.1, Seq(Map("freshness" -> "fresh", "eco" -> "yes", "price" -> "2 dkk"))),
        Row("orange", "orange", 0.2, Seq(Map("freshness" -> "too ripe", "eco" -> "no", "price" -> "1 dkk"))),
        Row("pomegranate", "red", 0.2, Seq(Map("freshness" -> "green", "eco" -> "yes", "price" -> "4 dkk")))))

    val newItemsDs = spark.createDataFrame(rows, fruitSchema)

    newItemsDs.printSchema()
    newItemsDs.show(false)

    newItemsDs.write.dynamodb(testTableName)

    println("Writing successful.")

    val validationDs = spark.read.schema(fruitSchema).dynamodb(testTableName)
    validationDs.show(false)
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select($"properties".as[Seq[Map[String, String]]])
        .collect()
        .forall(Seq(
          Seq(Map("freshness" -> "fresh", "eco" -> "yes", "price" -> "2 dkk")),
          Seq(Map("freshness" -> "too ripe", "eco" -> "no", "price" -> "1 dkk")),
          Seq(Map("freshness" -> "green", "eco" -> "yes", "price" -> "4 dkk"))) contains _))
  }

  test("Insert StructType") {
    import spark.implicits._

    val testTableName = "InsertTestStruct"
    createTestTable(testTableName)
    val fruitSchema = StructType(
      Seq(
        StructField("name", StringType, nullable = false),
        StructField("color", StringType, nullable = false),
        StructField("weight", DoubleType, nullable = false),
        StructField("freshness", StringType, nullable = false),
        StructField("eco", BooleanType, nullable = false),
        StructField("price", DoubleType, nullable = false)))

    val rows = spark.sparkContext.parallelize(
      Seq(Row("lemon", "yellow", 0.1, "fresh", true, 2.0), Row("pomegranate", "red", 0.2, "green", true, 4.0)))

    val newItemsDs = spark
      .createDataFrame(rows, fruitSchema)
      .select($"name", $"color", $"weight", struct($"freshness", $"eco", $"price") as "properties")

    newItemsDs.printSchema()
    newItemsDs.show(false)

    newItemsDs.write.dynamodb(testTableName)

    println("Writing successful.")

    val validationDs = spark.read.dynamodbAs[TestFruitWithProperties](testTableName)
    val validationDsCollect = validationDs.select($"properties".as[TestFruitProperties]).collect()
    assert(validationDs.count() === 2)
    assert(
      validationDs
        .select($"properties".as[TestFruitProperties])
        .collect()
        .forall(
          Seq(TestFruitProperties("fresh", eco = true, 2.0), TestFruitProperties("green", eco = true, 4.0)) contains _))
  }
}
