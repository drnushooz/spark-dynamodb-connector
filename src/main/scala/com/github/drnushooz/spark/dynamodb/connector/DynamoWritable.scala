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

import io.github.resilience4j.ratelimiter.RateLimiter
import org.apache.spark.sql.catalyst.InternalRow
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

private[dynamodb] trait DynamoWritable {
  val writeLimit: Double

  def putItems(columnSchema: ColumnSchema, items: Seq[InternalRow])(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit

  def updateItem(columnSchema: ColumnSchema, item: InternalRow)(client: DynamoDbClient, rateLimiter: RateLimiter): Unit

  def deleteItems(columnSchema: ColumnSchema, items: Seq[InternalRow])(
      client: DynamoDbClient,
      rateLimiter: RateLimiter): Unit
}
