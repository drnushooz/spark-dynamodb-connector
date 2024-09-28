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

import software.amazon.awssdk.core.pagination.sync.{PaginatedResponsesIterator, SdkIterable, SyncPageFetcher}
import software.amazon.awssdk.core.util.PaginatorUtils
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.internal.UserAgentUtils
import software.amazon.awssdk.services.dynamodb.model.{ExecuteStatementRequest, ExecuteStatementResponse}

class ExecuteStatementIterable(client: DynamoDbClient, request: ExecuteStatementRequest)
    extends SdkIterable[ExecuteStatementResponse] {
  private val firstRequest: ExecuteStatementRequest =
    UserAgentUtils.applyPaginatorUserAgent(request)
  private val nextPageFetcher: SyncPageFetcher[ExecuteStatementResponse] =
    new ExecuteStatementResponseFetcher

  override def iterator(): java.util.Iterator[ExecuteStatementResponse] = {
    val responsesIterator =
      PaginatedResponsesIterator.builder.nextPageFetcher(nextPageFetcher).build
    responsesIterator.asInstanceOf[java.util.Iterator[ExecuteStatementResponse]]
  }

  private class ExecuteStatementResponseFetcher extends SyncPageFetcher[ExecuteStatementResponse] {
    override def hasNextPage(previousPage: ExecuteStatementResponse): Boolean = {
      PaginatorUtils.isOutputTokenAvailable(previousPage.nextToken)
    }

    override def nextPage(previousPage: ExecuteStatementResponse): ExecuteStatementResponse = {
      Option(previousPage)
        .map(page => client.executeStatement(firstRequest.toBuilder.nextToken(page.nextToken).build))
        .getOrElse(client.executeStatement(firstRequest))
    }
  }
}
