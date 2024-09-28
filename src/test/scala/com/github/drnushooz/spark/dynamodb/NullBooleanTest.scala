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
import software.amazon.awssdk.services.dynamodb.model._

import scala.jdk.CollectionConverters._

class NullBooleanTest extends TestBase {
  test("Test Null") {
    import spark.implicits._

    val tableName = "TestNullBoolean"
    val createTableRequest = CreateTableRequest.builder
      .tableName(tableName)
      .attributeDefinitions(
        AttributeDefinition.builder
          .attributeName("Pk")
          .attributeType(ScalarAttributeType.S)
          .build)
      .keySchema(
        KeySchemaElement.builder
          .attributeName("Pk")
          .keyType(KeyType.HASH)
          .build)
      .provisionedThroughput(
        ProvisionedThroughput.builder
          .readCapacityUnits(5)
          .writeCapacityUnits(5)
          .build)
      .build
    client.createTable(createTableRequest)

    val testItems = Seq(("id1", "type1", true), ("id2", "type2", null))

    val items = testItems.map {
      case (_pk, _type, _value) if _type == "type1" =>
        Map(
          "Pk" -> AttributeValue.fromS(_pk),
          "Type" -> AttributeValue.fromS(_type),
          "_Value" -> AttributeValue.fromBool(_value.asInstanceOf[Boolean]))
      case (_pk, _type, _) =>
        Map(
          "Pk" -> AttributeValue.fromS(_pk),
          "Type" -> AttributeValue.fromS(_type),
          "_Value" -> AttributeValue.fromNul(true))
    }

    val writeRequests = items.map { it =>
      val putRequest = PutRequest.builder.item(it.asJava).build
      WriteRequest.builder.putRequest(putRequest).build
    }
    val batchWriteItemRequest = BatchWriteItemRequest.builder
      .requestItems(Map(tableName -> writeRequests.asJava).asJava)
      .build
    client.batchWriteItem(batchWriteItemRequest)

    val df = spark.read.dynamodbAs[BooleanClass](tableName)
    df.where($"Type" === "type2").show()

    deleteTestTable(tableName)
  }
}

case class BooleanClass(Pk: String, Type: String, _Value: Boolean)
