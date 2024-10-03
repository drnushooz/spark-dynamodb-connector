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

import com.github.drnushooz.spark.dynamodb.connector.DynamoTableConnector
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.types.StructType

case class DynamoBatchReader(
    connector: DynamoTableConnector,
    predicates: Array[Predicate],
    schema: StructType,
    limit: Option[Int])
    extends Scan
    with Batch
    with SupportsReportPartitioning {
  override def readSchema(): StructType = {
    schema
  }

  override def toBatch: Batch = {
    this
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val requiredColumns = schema.map(_.name)
    Array.tabulate(connector.totalSegments)(new ScanPartition(_, requiredColumns, predicates))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    DynamoReaderFactory(connector, schema, limit)
  }

  override val outputPartitioning: Partitioning = new OutputPartitioning(connector.totalSegments)
}
