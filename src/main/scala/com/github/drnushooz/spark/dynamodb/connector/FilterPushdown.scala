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

import org.apache.spark.sql.sources._

private[dynamodb] object FilterPushdown {
  def apply(filters: Seq[Filter]): String = {
    filters.map(buildCondition).mkString(" AND ")
  }

  /**
   * Accepts only filters that would be considered valid input to FilterPushdown.apply()
   *
   * @param filters
   *   input list which may contain both valid and invalid filters
   * @return
   *   a (valid, invalid) partitioning of the input filters
   */
  def acceptFilters(filters: Array[Filter]): (Array[Filter], Array[Filter]) =
    filters.partition(checkFilter)

  private def checkFilter(filter: Filter): Boolean = filter match {
    case _: StringEndsWith => false
    case And(left, right) => checkFilter(left) && checkFilter(right)
    case Or(left, right) => checkFilter(left) && checkFilter(right)
    case Not(f) => checkFilter(f)
    case _ => true
  }

  private def buildCondition(filter: Filter): String = filter match {
    case EqualTo(path, value: Boolean) => s"$path = $value"
    case EqualTo(path, value: String) => s"$path = '$value'"
    case EqualTo(path, value) => s"$path = $value"
    case GreaterThan(path, value) => s"$path > $value"
    case GreaterThanOrEqual(path, value) => s"$path >= $value"

    case LessThan(path, value) => s"$path < $value"
    case LessThanOrEqual(path, value) => s"$path <= $value"

    case In(path, values) =>
      val valueList = values.toList
      val valueExp = valueList match {
        case (_: String) :: _ =>
          s"[${valueList.toSet.mkString("'", "','", "'")}]"
        case (_: Boolean) :: _ | (_: Int) :: _ | (_: Long) :: _ | (_: Short) :: _ | (_: Float) :: _ |
            (_: Double) :: _ =>
          s"""[${valueList.toSet.mkString(",")}]"""
        case Nil =>
          throw new IllegalArgumentException("Unable to apply `In` filter with empty value list")
        case _ =>
          throw new IllegalArgumentException(
            s"Type of values supplied to `In` filter on attribute $path not " +
              s"supported by filter pushdown")
      }
      s"$path IN $valueExp"

    case IsNull(path) => s"$path IS NULL"
    case IsNotNull(path) => s"$path IS NOT NULL"
    case StringStartsWith(path, value) => s"""BEGINS_WITH("$path", '$value')"""
    case StringContains(path, value) => s"""CONTAINS("$path", '$value')"""

    case StringEndsWith(_, _) =>
      throw new UnsupportedOperationException(
        "Filter `StringEndsWith` is not supported by" +
          " DynamoDB")

    case And(left, right) => s"${buildCondition(left)} AND ${buildCondition(right)}"
    case Or(left, right) => s"${buildCondition(left)} OR ${buildCondition(right)}"
    case Not(f) => s"NOT ${buildCondition(f)}"
  }
}
