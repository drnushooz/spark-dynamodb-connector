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

import com.github.drnushooz.spark.dynamodb.connector.{DynamoTableConnector, PredicatePushdown}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._

class DynamoScanBuilder(connector: DynamoTableConnector, schema: StructType)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns
    with SupportsPushDownLimit {
  private var acceptedPredicates: Array[Predicate] = Array.empty
  private var currentSchema: StructType = schema
  private var scanLimit: Option[Int] = None

  override def build(): Scan = {
    DynamoBatchReader(connector, pushedPredicates(), currentSchema, scanLimit)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val keyFields =
      Seq(Some(connector.keySchema.hashKeyName), connector.keySchema.rangeKeyName).flatten
        .flatMap(keyName => currentSchema.fields.find(_.name == keyName))
    val requiredFields = keyFields ++ requiredSchema.fields
    val newFields = currentSchema.fields.filter(requiredFields.contains)
    currentSchema = StructType(newFields)
  }

  override def pushLimit(limit: Int): Boolean = {
    scanLimit = Some(limit)
    scanLimit.isDefined
  }

  override def isPartiallyPushed: Boolean = {
    scanLimit.isEmpty
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    if (connector.predicatePushdownEnabled) {
      val (acceptedPredicates, postScanPredicates) = PredicatePushdown.acceptPredicates(predicates)
      this.acceptedPredicates = acceptedPredicates
      postScanPredicates // Return filters that need to be evaluated after scanning.
    } else predicates
  }

  override def pushedPredicates(): Array[Predicate] = {
    acceptedPredicates
  }
}
