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

import com.github.drnushooz.spark.dynamodb.connector.{ColumnSchema, DynamoTableConnector}
import io.github.resilience4j.ratelimiter.{RateLimiter, RateLimiterConfig, RateLimiterRegistry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

import java.time.Duration

class DynamoDataUpdateWriter(columnSchema: ColumnSchema, connector: DynamoTableConnector, client: DynamoDbClient)
    extends DataWriter[InternalRow] {
  private val rateLimiterConfig = RateLimiterConfig.custom
    .limitRefreshPeriod(Duration.ofSeconds(1))
    .limitForPeriod(connector.writeLimit.toInt)
    .timeoutDuration(Duration.ofNanos(Long.MaxValue))
    .build
  private val rateLimiterRegistry = RateLimiterRegistry.of(rateLimiterConfig)
  private val rateLimiter: RateLimiter = rateLimiterRegistry.rateLimiter(this.getClass.getName)

  override def write(record: InternalRow): Unit = {
    connector.updateItem(columnSchema, record)(client, rateLimiter)
  }

  override def commit(): WriterCommitMessage = {
    DynamoCommitMessage()
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    client.close()
  }
}
