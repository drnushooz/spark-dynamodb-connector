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

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters._

class DynamoTable(options: CaseInsensitiveStringMap, userSchema: Option[StructType] = None)
    extends DynamoTableBase(options, userSchema) {
  override def attributeTypeMapping(
      inferenceItems: ArraySeq[java.util.Map[String, AttributeValue]]): Map[String, DataType] = {
    inferenceItems.foldLeft(Map[String, DataType]()) { case (map, item) =>
      map ++ item.asScala.mapValues(inferType)
    }
  }
}
