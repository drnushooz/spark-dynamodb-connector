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

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._

import java.net.URI
import scala.jdk.CollectionConverters._

class TestBase extends AnyFunSuite with BeforeAndAfterAll {
  val port = "8000"
  val uri = s"http://localhost:$port"
  val server: DynamoDBProxyServer = ServerRunner
    .createServerFromCommandLineArgs(Array("-inMemory", "-port", port))

  sys.props ++= Map(
    "aws.accessKeyId" -> "dummyKey",
    "aws.secretAccessKey" -> "dummySecret",
    "aws.dynamodb.endpoint" -> uri,
    "aws.dynamodb.region" -> Region.US_EAST_1.toString)

  val client: DynamoDbClient = DynamoDbClient.builder
    .httpClient(UrlConnectionHttpClient.builder.build)
    .endpointOverride(URI.create(System.getProperty("aws.dynamodb.endpoint", uri)))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
    .build

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName(this.getClass.getName)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val testTableName: String = "TestFruit"

  override def beforeAll(): Unit = {
    server.start()

    // Create a test table.
    createTestTable(testTableName)

    // Populate with test data.
    populateTestTable(testTableName)
  }

  override def afterAll(): Unit = {
    deleteTestTable(testTableName)
    server.stop()
  }

  def createTestTable(tableName: String, ddbClient: DynamoDbClient = client): CreateTableResponse = {
    val createTableRequest = CreateTableRequest.builder
      .tableName(tableName)
      .attributeDefinitions(
        AttributeDefinition.builder
          .attributeName("name")
          .attributeType(ScalarAttributeType.S)
          .build)
      .keySchema(
        KeySchemaElement.builder
          .attributeName("name")
          .keyType(KeyType.HASH)
          .build)
      .provisionedThroughput(
        ProvisionedThroughput.builder
          .readCapacityUnits(5)
          .writeCapacityUnits(5)
          .build)
      .build
    ddbClient.createTable(createTableRequest)
  }

  def populateTestTable(tableName: String, ddbClient: DynamoDbClient = client): BatchWriteItemResponse = {
    val testItems = Seq(
      ("apple", "red", 0.2),
      ("banana", "yellow", 0.15),
      ("watermelon", "red", 0.5),
      ("grape", "green", 0.01),
      ("pear", "green", 0.2),
      ("kiwi", "green", 0.05),
      ("blackberry", "purple", 0.01),
      ("blueberry", "purple", 0.01),
      ("plum", "purple", 0.1))

    val writeRequests = testItems.map { case (name, color, weight) =>
      val attributes = Map(
        "name" -> AttributeValue.fromS(name),
        "color" -> AttributeValue.fromS(color),
        "weightKg" -> AttributeValue.fromN(String.valueOf(weight)))
      val putRequest = PutRequest.builder.item(attributes.asJava).build
      WriteRequest.builder.putRequest(putRequest).build
    }

    val batchWriteItemRequest = BatchWriteItemRequest.builder
      .requestItems(Map(tableName -> writeRequests.asJava).asJava)
      .build
    ddbClient.batchWriteItem(batchWriteItemRequest)
  }

  def deleteTestTable(tableName: String, ddbClient: DynamoDbClient = client): DeleteTableResponse = {
    val deleteTableRequest = DeleteTableRequest.builder.tableName(tableName).build
    ddbClient.deleteTable(deleteTableRequest)
  }
}
