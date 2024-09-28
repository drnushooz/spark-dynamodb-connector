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
package com.github.drnushooz.spark.dynamodb.connector

import com.github.drnushooz.spark.dynamodb.catalyst.DynamoTypeConversions
import io.github.resilience4j.ratelimiter.RateLimiter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.net.URI
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

private[dynamodb] case class DynamoTableConnector(
    tableName: String,
    parallelism: Int,
    parameters: Map[String, String],
    indexName: Option[String] = None)
    extends DynamoWritable
    with Serializable {
  private val consistentRead = parameters.getOrElse("stronglyConsistentReads", "false").toBoolean
  private val filterPushdown = parameters.getOrElse("filterPushdown", "true").toBoolean
  private val region = parameters.get("region")
  private val roleArn = parameters.get("roleArn")
  private val providerClassName = parameters.get("providerClassName")

  @transient private lazy val properties = sys.props

  val (keySchema, readLimit, writeLimit, itemLimit, totalSegments) = {
    val client = getDynamoDB(region, roleArn, providerClassName)
    val request = DescribeTableRequest.builder
      .tableName(tableName)
      .build
    val desc = client.describeTable(request)
    val dynamoTable = desc.table
    val indexDesc = indexName.flatMap { in =>
      dynamoTable.globalSecondaryIndexes.asScala.find(_.indexName == in)
    }

    // Key schema.
    val keySchema = KeySchema.fromDescription(dynamoTable.keySchema.asScala.toSeq)

    // User parameters.
    val bytesPerRCU = parameters.getOrElse("bytesPerRCU", "4096").toInt
    val maxPartitionBytes = parameters.getOrElse("maxPartitionBytes", "134217728").toInt
    val targetCapacity = parameters.getOrElse("targetCapacity", "1").toDouble
    val readFactor = if (consistentRead) 1 else 2

    // Table parameters.
    val (tableSize, itemCount) = indexDesc
      .map(iDesc => (iDesc.indexSizeBytes, iDesc.itemCount))
      .getOrElse((dynamoTable.tableSizeBytes, dynamoTable.itemCount))

    // Partitioning calculation.
    val totalSize = indexDesc.map(_.indexSizeBytes).getOrElse(tableSize)
    val numPartitions = parameters.get("readPartitions").map(_.toInt).getOrElse {
      val sizeBased = (totalSize / maxPartitionBytes).toInt max 1
      val remainder = sizeBased % parallelism
      if (remainder > 0) sizeBased + (parallelism - remainder)
      else sizeBased
    }

    // Provisioned or on-demand throughput.
    val readThroughput = parameters
      .getOrElse(
        "throughput",
        Option(dynamoTable.provisionedThroughput.readCapacityUnits)
          .filter(_ > 0)
          .map(_.longValue.toString)
          .getOrElse("100"))
      .toLong
    val writeThroughput = parameters
      .getOrElse(
        "throughput",
        Option(dynamoTable.provisionedThroughput.writeCapacityUnits)
          .filter(_ > 0)
          .map(_.longValue.toString)
          .getOrElse("100"))
      .toLong

    // Rate limit calculation.
    val avgItemSize = totalSize.toDouble / itemCount
    val readCapacity = readThroughput * targetCapacity
    val writeCapacity = writeThroughput * targetCapacity

    val readLimit = readCapacity / parallelism
    val itemLimit = ((bytesPerRCU / avgItemSize * readLimit).toInt * readFactor) max 1
    val writeLimit = writeCapacity / parallelism

    (keySchema, readLimit, writeLimit, itemLimit, numPartitions)
  }

  def isEmpty: Boolean = itemLimit == 0

  def nonEmpty: Boolean = !isEmpty

  def filterPushdownEnabled: Boolean = filterPushdown

  def getDynamoDB(
      region: Option[String] = None,
      roleArn: Option[String] = None,
      providerClassName: Option[String] = None): DynamoDbClient = {
    getDynamoDBClient(region, roleArn, providerClassName)
  }

  private def getDynamoDBClient(
      region: Option[String] = None,
      roleArn: Option[String] = None,
      providerClassName: Option[String]): DynamoDbClient = {
    val chosenRegion =
      region.getOrElse(properties.getOrElse("aws.dynamodb.region", Region.US_EAST_1.toString))
    val credentials = getCredentials(chosenRegion, roleArn, providerClassName)

    properties
      .get("aws.dynamodb.endpoint")
      .map { endpoint =>
        DynamoDbClient.builder
          .credentialsProvider(credentials)
          .region(Region.of(chosenRegion))
          .endpointOverride(URI.create(endpoint))
          .build
      }
      .getOrElse {
        DynamoDbClient.builder
          .credentialsProvider(credentials)
          .region(Region.of(chosenRegion))
          .build
      }
  }

  def getDynamoDBAsyncClient(
      region: Option[String] = None,
      roleArn: Option[String] = None,
      providerClassName: Option[String] = None): DynamoDbAsyncClient = {
    val chosenRegion =
      region.getOrElse(properties.getOrElse("aws.dynamodb.region", Region.US_EAST_1.toString))
    val credentials = getCredentials(chosenRegion, roleArn, providerClassName)

    properties
      .get("aws.dynamodb.endpoint")
      .map { endpoint =>
        DynamoDbAsyncClient.builder
          .credentialsProvider(credentials)
          .region(Region.of(chosenRegion))
          .endpointOverride(URI.create(endpoint))
          .build
      }
      .getOrElse {
        DynamoDbAsyncClient.builder
          .credentialsProvider(credentials)
          .region(Region.of(chosenRegion))
          .build
      }
  }

  /**
   * Get credentials from an instantiated object of the class name given or a passed in arn or from profile or return
   * the default credential provider
   */
  private def getCredentials(
      chosenRegion: String,
      roleArn: Option[String],
      providerClassName: Option[String]): AwsCredentialsProvider = {
    providerClassName
      .map { providerClass =>
        Class
          .forName(providerClass)
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[AwsCredentialsProvider]
      }
      .orElse(roleArn.map { arn =>
        val stsClient = properties
          .get("aws.sts.endpoint")
          .map(endpoint => {
            StsClient.builder
              .credentialsProvider(DefaultCredentialsProvider.create)
              .endpointOverride(URI.create(endpoint))
              .region(Region.of(chosenRegion))
              .build
          })
          .getOrElse {
            // STS without an endpoint will sign from the region, but use the global endpoint
            StsClient.builder
              .credentialsProvider(DefaultCredentialsProvider.create)
              .region(Region.of(chosenRegion))
              .build
          }
        val assumeRoleResult = stsClient.assumeRole(
          AssumeRoleRequest.builder
            .roleSessionName("DynamoDBAssumed")
            .roleArn(arn)
            .build)
        val stsCredentials = assumeRoleResult.credentials
        val assumeCredentials = AwsSessionCredentials.builder
          .accessKeyId(stsCredentials.accessKeyId)
          .secretAccessKey(stsCredentials.secretAccessKey)
          .sessionToken(stsCredentials.sessionToken)
          .build
        StaticCredentialsProvider.create(assumeCredentials)
      })
      .orElse(properties.get("aws.profile").map(ProfileCredentialsProvider.create))
      .getOrElse(DefaultCredentialsProvider.create)
  }

  def scan(columns: Seq[String], filters: Seq[Filter]): ExecuteStatementIterable = {
    val projectionBuf = new StringBuilder
    if (columns.nonEmpty)
      projectionBuf ++= s"SELECT ${columns.mkString(",")} "
    else
      projectionBuf ++= "SELECT * "

    projectionBuf ++= indexName
      .map(in => s"""FROM "$tableName"."$in"""")
      .getOrElse(s"""FROM "$tableName"""")

    if (filters.nonEmpty && filterPushdown)
      projectionBuf ++= s" WHERE ${FilterPushdown(filters)}"
    projectionBuf ++= ";"

    val executeStatementRequest = ExecuteStatementRequest.builder
      .statement(projectionBuf.toString)
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .consistentRead(consistentRead)
      .build

    val ddbClient = getDynamoDB(region, roleArn, providerClassName)
    new ExecuteStatementIterable(ddbClient, executeStatementRequest)
  }

  override def putItems(columnSchema: ColumnSchema, rows: Seq[InternalRow])(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit = {
    putOrDeleteInternal(columnSchema, rows)(client, rateLimiter)
  }

  override def updateItem(columnSchema: ColumnSchema, row: InternalRow)(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit = {
    // Map primary key.
    val keyColumns = columnSchema.keys() match {
      case Left((hashKey, hashKeyIndex, hashKeyType)) =>
        Map(hashKey -> DynamoTypeConversions.convertRowValue(row, hashKeyIndex, hashKeyType))
      case Right(((hashKey, hashKeyIndex, hashKeyType), (rangeKey, rangeKeyIndex, rangeKeyType))) =>
        val hashKeyValue = DynamoTypeConversions.convertRowValue(row, hashKeyIndex, hashKeyType)
        val rangeKeyValue =
          DynamoTypeConversions.convertRowValue(row, rangeKeyIndex, rangeKeyType)
        Map(hashKey -> hashKeyValue, rangeKey -> rangeKeyValue)
    }

    // Map remaining columns.
    val updateColumns = columnSchema
      .attributes()
      .collect {
        case (name, index, dataType) if !row.isNullAt(index) =>
          val valueUpdate = AttributeValueUpdate.builder
            .value(DynamoTypeConversions.convertRowValue(row, index, dataType))
            .action(AttributeAction.PUT)
            .build
          name -> valueUpdate
      }
      .toMap

    // Update item and rate limit on write capacity.
    val updateItemRequest = UpdateItemRequest.builder
      .tableName(tableName)
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .key(keyColumns.asJava)
      .attributeUpdates(updateColumns.asJava)
      .build

    val updateItemResponse = client.updateItem(updateItemRequest)
    Option(updateItemResponse.consumedCapacity).foreach { cap =>
      rateLimiter.acquirePermission(cap.capacityUnits.toInt max 1)
    }
  }

  override def deleteItems(columnSchema: ColumnSchema, rows: Seq[InternalRow])(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit = {
    putOrDeleteInternal(columnSchema, rows, isDelete = true)(client, rateLimiter)
  }

  private def putOrDeleteInternal(columnSchema: ColumnSchema, rows: Seq[InternalRow], isDelete: Boolean = false)(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit = {
    // Check if hash key only or also range key.
    val writeRequests = rows.map { internalRow =>
      // Map primary key.
      val keyColumns = columnSchema.keys() match {
        case Left((hashKey, hashKeyIndex, hashKeyType)) =>
          Map(
            hashKey -> DynamoTypeConversions
              .convertRowValue(internalRow, hashKeyIndex, hashKeyType))
        case Right(((hashKey, hashKeyIndex, hashKeyType), (rangeKey, rangeKeyIndex, rangeKeyType))) =>
          val hashKeyValue =
            DynamoTypeConversions.convertRowValue(internalRow, hashKeyIndex, hashKeyType)
          val rangeKeyValue =
            DynamoTypeConversions.convertRowValue(internalRow, rangeKeyIndex, rangeKeyType)
          Map(hashKey -> hashKeyValue, rangeKey -> rangeKeyValue)
      }

      if (isDelete) {
        val dRequest = DeleteRequest.builder.key(keyColumns.asJava).build
        WriteRequest.builder.deleteRequest(dRequest).build
      } else {
        // Map remaining columns.
        val otherColumns = columnSchema
          .attributes()
          .collect {
            case (name, index, dataType) if !internalRow.isNullAt(index) =>
              name -> DynamoTypeConversions.convertRowValue(internalRow, index, dataType)
          }
          .toMap

        val columnsToWrite = keyColumns ++ otherColumns
        val pRequest = PutRequest.builder.item(columnsToWrite.asJava).build
        WriteRequest.builder.putRequest(pRequest).build
      }
    }

    val batchWriteItemRequest = BatchWriteItemRequest.builder
      .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
      .requestItems(Map(tableName -> writeRequests.asJava).asJava)
      .build
    val response = client.batchWriteItem(batchWriteItemRequest)
    handleBatchWriteResponse(client, rateLimiter)(response)
  }

  @tailrec
  private def handleBatchWriteResponse(client: DynamoDbClient, rateLimiter: RateLimiter)(
      response: BatchWriteItemResponse): Unit = {
    // Rate limit on write capacity.
    if (response.hasConsumedCapacity) {
      val capMap = response.consumedCapacity.asScala.map { cap =>
        cap.tableName -> cap.capacityUnits.toInt
      }.toMap
      capMap.get(tableName).foreach(units => rateLimiter.acquirePermission(units max 1))
    }

    // Retry unprocessed items.
    if (response.hasUnprocessedItems && !response.unprocessedItems.isEmpty) {
      val batchWriteItemRequest = BatchWriteItemRequest.builder
        .requestItems(response.unprocessedItems)
        .build
      val newResponse = client.batchWriteItem(batchWriteItemRequest)
      handleBatchWriteResponse(client, rateLimiter)(newResponse)
    }
  }
}
