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
package com.github.drnushooz.spark.dynamodb.catalyst

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import scala.collection.compat.immutable._
import scala.jdk.CollectionConverters._

object DynamoTypeConversions {
  def convertRowValue(row: InternalRow, index: Int, elementType: DataType): AttributeValue = {
    elementType match {
      case ArrayType(innerType, _) => convertArray(row.getArray(index), innerType)
      case MapType(keyType, valueType, _) => convertMap(row.getMap(index), keyType, valueType)
      case StructType(fields) =>
        convertStruct(row.getStruct(index, fields.length), ArraySeq.unsafeWrapArray(fields))
      case _ => convertCatalystToDynamo(row, index, elementType)
    }
  }

  private def convertCatalystToDynamo(row: InternalRow, index: Int, elementType: DataType): AttributeValue = {
    elementType match {
      case BooleanType => AttributeValue.fromBool(row.getBoolean(index))
      case BinaryType => AttributeValue.fromB(SdkBytes.fromByteArray(row.getBinary(index)))
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DecimalType() =>
        val nValue =
          CatalystTypeConverters.convertToScala(row.get(index, elementType), elementType).toString
        AttributeValue.fromN(nValue)
      case _ =>
        val sValue =
          CatalystTypeConverters.convertToScala(row.get(index, elementType), elementType).toString
        AttributeValue.fromS(sValue)
    }
  }

  private def convertCatalystToDynamo(value: Any, elementType: DataType): AttributeValue = {
    val cValue = CatalystTypeConverters.convertToScala(value, elementType)
    elementType match {
      case BooleanType => AttributeValue.fromBool(cValue.asInstanceOf[Boolean])
      case BinaryType =>
        AttributeValue.fromB(SdkBytes.fromByteArray(cValue.asInstanceOf[Array[Byte]]))
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DecimalType() =>
        AttributeValue.fromN(cValue.toString)
      case _ => AttributeValue.fromS(cValue.toString)
    }
  }

  private def convertArray(array: ArrayData, elementType: DataType): AttributeValue = {
    val cValue = elementType match {
      case ArrayType(innerType, _) =>
        array.toSeq[ArrayData](elementType).map(convertArray(_, innerType))
      case MapType(keyType, valueType, _) =>
        array.toSeq[MapData](elementType).map(convertMap(_, keyType, valueType))
      case structType: StructType =>
        array
          .toSeq[InternalRow](structType)
          .map(convertStruct(_, ArraySeq.unsafeWrapArray(structType.fields)))
      case StringType =>
        convertStringArray(array).map(AttributeValue.fromS)
      case _ =>
        array.toSeq[Any](elementType).map(convertCatalystToDynamo(_, elementType))
    }

    AttributeValue.fromL(cValue.asJava)
  }

  private def convertMap(map: MapData, keyType: DataType, valueType: DataType): AttributeValue = {
    if (keyType != StringType)
      throw new IllegalArgumentException(
        s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
    val keys = convertStringArray(map.keyArray())
    val values = convertArray(map.valueArray(), valueType).l
    AttributeValue.fromM(Map.from(keys.zip(values.asScala)).asJava)
  }

  private def convertStruct(row: InternalRow, fields: Seq[StructField]): AttributeValue = {
    val kvPairs = LazyList.range(0, row.numFields).map {
      case i if row.isNullAt(i) => fields(i).name -> null
      case i => fields(i).name -> convertRowValue(row, i, fields(i).dataType)
    }
    AttributeValue.fromM(Map.from(kvPairs).asJava)
  }

  private def convertStringArray(array: ArrayData): Seq[String] = {
    ArraySeq.unsafeWrapArray(array.toArray[UTF8String](StringType).map(_.toString))
  }
}
