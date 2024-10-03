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

import com.github.drnushooz.spark.dynamodb.reflect.SchemaAnalysis
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col

import scala.collection.compat.immutable.ArraySeq
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object implicits {
  implicit class DynamoDBDataFrameReader(reader: DataFrameReader) {
    def dynamodb(tableName: String): DataFrame = {
      getDynamoDBSource(tableName).load()
    }

    def dynamodb(tableName: String, indexName: String): DataFrame = {
      getDynamoDBSource(tableName).option("indexName", indexName).load()
    }

    def dynamodbAs[T <: Product: ClassTag: TypeTag](tableName: String): Dataset[T] = {
      implicit val encoder: Encoder[T] = ExpressionEncoder()
      val (schema, aliasMap) = SchemaAnalysis[T]
      getColumnsAlias(getDynamoDBSource(tableName).schema(schema).load(), aliasMap).as
    }

    def dynamodbAs[T <: Product: ClassTag: TypeTag](tableName: String, indexName: String): Dataset[T] = {
      implicit val encoder: Encoder[T] = ExpressionEncoder()
      val (schema, aliasMap) = SchemaAnalysis[T]
      getColumnsAlias(getDynamoDBSource(tableName).option("indexName", indexName).schema(schema).load(), aliasMap).as
    }

    private def getDynamoDBSource(tableName: String): DataFrameReader = {
      reader
        .format("com.github.drnushooz.spark.dynamodb.datasource")
        .option("tableName", tableName)
    }

    private def getColumnsAlias(dataFrame: DataFrame, aliasMap: Map[String, String]): DataFrame = {
      if (aliasMap.isEmpty) {
        dataFrame
      } else {
        val columnsAlias = dataFrame.columns.map {
          case name if aliasMap.isDefinedAt(name) => col(name) as aliasMap(name)
          case name => col(name)
        }
        dataFrame.select(ArraySeq.unsafeWrapArray(columnsAlias): _*)
      }
    }
  }

  implicit class DynamoDBDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def dynamodb(tableName: String): Unit =
      writer
        .format("com.github.drnushooz.spark.dynamodb.datasource")
        .mode(SaveMode.Append)
        .option("tableName", tableName)
        .save()

  }
}
