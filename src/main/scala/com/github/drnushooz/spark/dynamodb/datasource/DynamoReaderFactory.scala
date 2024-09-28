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
package com.github.drnushooz.spark.dynamodb.datasource

import com.github.drnushooz.spark.dynamodb.catalyst.SparkTypeConversions
import com.github.drnushooz.spark.dynamodb.connector.DynamoTableConnector
import io.github.resilience4j.ratelimiter.{RateLimiterConfig, RateLimiterRegistry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.{StructField, StructType}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.time.Duration
import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters._

case class DynamoReaderFactory(connector: DynamoTableConnector, schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    if (connector.isEmpty) new EmptyReader
    else new ScanPartitionReader(partition.asInstanceOf[ScanPartition])
  }

  private class EmptyReader extends PartitionReader[InternalRow] {
    override def next(): Boolean = false

    override def get(): InternalRow = throw new IllegalStateException("Unable to call get() on empty iterator")

    override def close(): Unit = {}
  }

  private class ScanPartitionReader(scanPartition: ScanPartition) extends PartitionReader[InternalRow] {
    import scanPartition._

    private val responseIterator = connector
      .scan(requiredColumns, ArraySeq.unsafeWrapArray(filters))
      .iterator()

    private val rateLimiterConfig = RateLimiterConfig.custom
      .limitRefreshPeriod(Duration.ofSeconds(1))
      .limitForPeriod(connector.readLimit.toInt)
      .timeoutDuration(Duration.ofNanos(Long.MaxValue))
      .build
    private val rateLimiterRegistry = RateLimiterRegistry.of(rateLimiterConfig)
    private val rateLimiter = rateLimiterRegistry.rateLimiter(this.getClass.getName)

    private var innerIterator: Iterator[InternalRow] = Iterator.empty

    private var currentRow: InternalRow = _
    private var proceed = false

    private val typeConversions = schema.collect { case StructField(name, dataType, _, _) =>
      name -> SparkTypeConversions(name, dataType)
    }.toMap

    override def next(): Boolean = {
      proceed = true
      innerIterator.hasNext || {
        if (responseIterator.hasNext) {
          nextPage()
          next()
        } else false
      }
    }

    override def get(): InternalRow = {
      if (proceed) {
        currentRow = innerIterator.next()
        proceed = false
      }
      currentRow
    }

    override def close(): Unit = {}

    private def nextPage(): Unit = {
      val response = responseIterator.next()
      Option(response.consumedCapacity).foreach(cap => rateLimiter.acquirePermission(cap.capacityUnits.toInt max 1))
      val items = response.items.asScala.map(item => Map.from(item.asScala))
      innerIterator = items.map(itemToRow(requiredColumns)).iterator
    }

    private def itemToRow(requiredColumns: Seq[String])(item: Map[String, AttributeValue]): InternalRow = {
      if (requiredColumns.nonEmpty) {
        InternalRow.fromSeq(requiredColumns.map(columnName => typeConversions(columnName)(item)))
      } else {
        InternalRow.fromSeq(item.values.map(_.toString).toSeq)
      }
    }
  }
}
