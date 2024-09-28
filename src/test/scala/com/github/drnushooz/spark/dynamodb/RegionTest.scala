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
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.net.URI

class RegionTest extends TestBase {
  test("Inserting from a local Dataset") {
    import spark.implicits._

    val tableName = "RegionTest1"
    createTestTable(tableName)

    val clientEU: DynamoDbClient = DynamoDbClient.builder
      .httpClient(UrlConnectionHttpClient.builder.build)
      .endpointOverride(URI.create(System.getProperty("aws.dynamodb.endpoint")))
      .region(Region.EU_CENTRAL_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
      .build

    createTestTable(tableName, clientEU)

    val testRegion = Region.EU_CENTRAL_1.toString
    val newItemsDs = spark
      .createDataset(Seq(("lemon", "yellow", 0.1), ("orange", "orange", 0.2), ("pomegranate", "red", 0.2)))
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "color")
      .withColumnRenamed("_3", "weight")
    newItemsDs.write.option("region", testRegion).dynamodb(tableName)

    val validationDs = spark.read.dynamodb(tableName)
    assert(validationDs.count() === 0)
    val validationDsEU = spark.read.option("region", testRegion).dynamodb(tableName)
    assert(validationDsEU.count() === 3)
  }
}
