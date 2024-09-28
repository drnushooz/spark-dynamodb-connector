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
import org.apache.spark.sql.functions.{lit, when, length => sqlLength}
import org.scalatest.matchers.should._
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class WriteRelationTest extends TestBase with Matchers {
  test("Inserting from a local Dataset") {
    import spark.implicits._

    val tableName = "InsertTest1"
    createTestTable(tableName)

    val newItemsDs = spark
      .createDataset(Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2)))
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "color")
      .withColumnRenamed("_3", "weight")
    newItemsDs.write.dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select("name")
        .as[String]
        .collect()
        .forall(Seq("lemon", "orange", "pomegranate") contains _))
    assert(
      validationDs
        .select("color")
        .as[String]
        .collect()
        .forall(Seq("yellow", "orange", "red") contains _))
    assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.1, 0.2, 0.2) contains _))
  }

  test("Deleting from a local Dataset with a HashKey only") {
    import spark.implicits._

    val tableName = "DeleteTest1"
    createTestTable(tableName)

    val newItemsDs = Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2)).toDF(
      "name",
      "color",
      "weight")
    newItemsDs.write.dynamodb(tableName)

    val toDelete = Seq(("lemon", "yellow"), ("orange", "blue"), ("doesn't exist", "black"))
      .toDF("name", "color")
    toDelete.write.option("delete", "true").dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    validationDs.count() shouldEqual 1
    val rec = validationDs.first()
    rec.getString(rec.fieldIndex("name")) shouldEqual "pomegranate"
    rec.getString(rec.fieldIndex("color")) shouldEqual "red"
    rec.getDouble(rec.fieldIndex("weight")) shouldEqual 0.2
  }

  test("Deleting from a local Dataset with a HashKey and RangeKey") {
    import spark.implicits._

    val tableName = "DeleteTest2"

    val keys = Map("name" -> (ScalarAttributeType.S, KeyType.HASH), "weight" -> (ScalarAttributeType.N, KeyType.RANGE))

    val keyDefinitions = keys.map { case (k, (st, _)) =>
      AttributeDefinition.builder.attributeName(k).attributeType(st).build
    }
    val keyType = keys.map { case (k, (_, kt)) =>
      KeySchemaElement.builder.attributeName(k).keyType(kt).build
    }

    val createTableRequest = CreateTableRequest.builder
      .tableName(tableName)
      .attributeDefinitions(keyDefinitions.asJavaCollection)
      .keySchema(keyType.asJavaCollection)
      .provisionedThroughput(
        ProvisionedThroughput.builder
          .readCapacityUnits(5)
          .writeCapacityUnits(5)
          .build)
      .build
    client.createTable(createTableRequest)

    val newItemsDs =
      Seq(("lemon", "yellow", 0.1), ("lemon", "blue", 4.0), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2))
        .toDF("name", "color", "weight")
    newItemsDs.write.dynamodb(tableName)

    val toDelete = Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "shouldn'tdelete", 0.5))
      .toDF("name", "color", "weight")
    toDelete.write.option("delete", "true").dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    validationDs.show()
    validationDs.count() shouldEqual 2
    validationDs.select("name").as[String].collect() should contain theSameElementsAs Seq("lemon", "pomegranate")
    validationDs.select("color").as[String].collect() should contain theSameElementsAs Seq("blue", "red")
  }

  test("Updating from a local Dataset with new and only some previous columns") {
    import spark.implicits._

    val tableName = "UpdateTest1"
    createTestTable(tableName)

    val newItemsDs = Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2)).toDF(
      "name",
      "color",
      "weight")
    newItemsDs.write.dynamodb(tableName)

    newItemsDs
      .withColumn("color_size", sqlLength($"color"))
      .drop("color")
      .withColumn("weight", $"weight" * 2)
      .write
      .option("update", "true")
      .dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    validationDs.show()
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select("name")
        .as[String]
        .collect()
        .forall(Seq("lemon", "orange", "pomegranate") contains _))
    assert(
      validationDs
        .select("color")
        .as[String]
        .collect()
        .forall(Seq("yellow", "orange", "red") contains _))
    assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.2, 0.4, 0.4) contains _))
    assert(validationDs.select("color_size").as[Long].collect().forall(Seq(6, 3) contains _))
  }

  test("Updating from a local Dataset with null values") {
    import spark.implicits._

    val tableName = "UpdateTest2"
    createTestTable(tableName)

    val newItemsDs = Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2)).toDF(
      "name",
      "color",
      "weight")
    newItemsDs.write.dynamodb(tableName)

    val alteredDs = newItemsDs
      .withColumn("weight", when($"weight" < 0.2, $"weight").otherwise(lit(null)))
    alteredDs.show()
    alteredDs.write.option("update", "true").dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    validationDs.show()
    assert(validationDs.count() === 3)
    assert(
      validationDs
        .select("name")
        .as[String]
        .collect()
        .forall(Seq("lemon", "orange", "pomegranate") contains _))
    assert(
      validationDs
        .select("color")
        .as[String]
        .collect()
        .forall(Seq("yellow", "orange", "red") contains _))
    assert(validationDs.select("weight").as[Double].collect().forall(Seq(0.2, 0.1) contains _))
  }
}
