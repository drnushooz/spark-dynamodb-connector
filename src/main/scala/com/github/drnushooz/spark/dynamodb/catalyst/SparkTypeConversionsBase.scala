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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type

import scala.jdk.CollectionConverters._

private[dynamodb] class SparkTypeConversionsBase {
  def apply(attrName: String, sparkType: DataType): Map[String, AttributeValue] => Any = { item =>
    {
      sparkType match {
        case BooleanType | ByteType | BinaryType | ShortType | IntegerType | LongType | FloatType | DoubleType |
            DecimalType() | StringType =>
          item.get(attrName).map(convertAttribute(sparkType)).orNull
        case ArrayType(innerType, _) =>
          item
            .get(attrName)
            .map(convertAttribute(innerType))
            .map(extractArray(convertValue(innerType)))
            .orNull
        case MapType(keyType, valueType, _) =>
          if (keyType != StringType) {
            throw new IllegalArgumentException(
              s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports " +
                s"String as Map key type.")
          }
          item
            .get(attrName)
            .map(convertAttribute(valueType))
            .map(extractMap(convertValue(valueType)))
            .orNull
        case StructType(fields) =>
          val nestedTypes = fields.collect { case StructField(name, dataType, _, _) =>
            name -> dataType
          }.toMap
          val nestedConversions = createNestedConversions(nestedTypes)
          val dAttributes = item.get(attrName).map(_.m.asScala)
          val attributes = dAttributes.map(_.map { case (name, attr) =>
            name -> convertAttribute(nestedTypes(name))(attr)
          }.asJava)
          attributes.map(extractStruct(nestedConversions)).orNull
        case _ =>
          throw new IllegalArgumentException(
            s"Spark DataType '${sparkType.typeName}' could not be mapped to a " +
              s"corresponding DynamoDB data type.")
      }
    }
  }

  private val stringConverter = (value: Any) => UTF8String.fromString(value.asInstanceOf[String])

  /**
   * Maps and Lists in DynamoDB can have heterogeneous value types. Hence when converting them to destination Spark
   * type, ensure that AttributeValue.type() is taken into consideration in conjunction with sparkType
   */
  protected def convertAttribute(sparkType: DataType): AttributeValue => Any = { value =>
    {
      value.`type` match {
        case Type.S =>
          UTF8String.fromString(value.s)
        case Type.N =>
          val nValue = BigDecimal(value.n)
          sparkType match {
            case ByteType => nValue.toByte
            case ShortType => nValue.toShort
            case IntegerType => nValue.toInt
            case LongType => nValue.toLong
            case FloatType => nValue.toFloat
            case DoubleType => nValue.toDouble
            case _ => nValue
          }
        case Type.BOOL => value.bool
        case Type.NUL => value.nul
        case Type.M => extractMapAttributes(value, sparkType)
        case Type.L => value.l.asScala.map(convertAttribute(sparkType)).asJava
        case Type.B => value.b.asByteArray
        case Type.SS => value.ss
        case Type.NS => value.ns
        case Type.BS => java.util.Arrays.asList(value.b.asByteArray())
        case _ => StringType
      }
    }
  }

  protected def convertValue(sparkType: DataType): Any => Any = {
    sparkType match {
      case IntegerType => _.toString.toInt
      case LongType => _.toString.toLong
      case DoubleType => _.toString.toDouble
      case FloatType => _.toString.toFloat
      case DecimalType() => identity
      case ArrayType(innerType, _) => extractArray(convertValue(innerType))
      case MapType(keyType, valueType, _) =>
        if (keyType != StringType) {
          throw new IllegalArgumentException(
            s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports " +
              s"String as Map key type.")
        }
        extractMap(convertValue(valueType))
      case StructType(fields) =>
        val nestedConversions = fields.collect { case StructField(name, dataType, _, _) =>
          name -> convertValue(dataType)
        }.toMap
        extractStruct(nestedConversions)
      case BooleanType => {
        case boolean: Boolean => boolean
        case v =>
          throw new IllegalArgumentException(
            s"Spark DataType '${sparkType.typeName}' could not be mapped to '${v.getClass.getCanonicalName}'.")
      }
      case StringType => {
        case string: String => UTF8String.fromString(string)
        case utf8String: UTF8String => utf8String
        case v =>
          throw new IllegalArgumentException(
            s"Spark DataType '${sparkType.typeName}' could not be mapped to '${v.getClass.getCanonicalName}'.")
      }
      case BinaryType => {
        case sdkBytes: SdkBytes => sdkBytes.asByteArray
        case v =>
          throw new IllegalArgumentException(
            s"Spark DataType '${sparkType.typeName}' could not be mapped to '${v.getClass.getCanonicalName}'.")
      }
      case v =>
        throw new IllegalArgumentException(
          s"Spark DataType '${sparkType.typeName}' could not be mapped to '${v.getClass.getCanonicalName}'.")
    }
  }

  private def extractArray(converter: Any => Any): Any => Any = {
    case list: java.util.List[_] => new GenericArrayData(list.asScala.map(converter))
    case set: java.util.Set[_] => new GenericArrayData(set.asScala.map(converter))
    case v =>
      throw new IllegalArgumentException(s"Unexpected type '${v.getClass.getCanonicalName}' in extractArray.")
  }

  private def extractMap(converter: Any => Any): Any => Any = {
    case map: java.util.Map[_, _] => ArrayBasedMapData(map, stringConverter, converter)
    case v =>
      throw new IllegalArgumentException(s"Unexpected type '${v.getClass.getCanonicalName}' in extractMap.")
  }

  private def extractStruct(conversions: Map[String, Any => Any]): Any => Any = {
    case map: java.util.Map[_, _] =>
      InternalRow.fromSeq(conversions.map { case (name, conv) =>
        conv(map.get(name))
      }.toSeq)
    case v =>
      throw new IllegalArgumentException(s"Unexpected type '${v.getClass.getCanonicalName}' in extractStruct.")
  }

  def extractMapAttributes(value: AttributeValue, sparkType: DataType): Any = {}

  def createNestedConversions(nestedTypes: Map[String, DataType]): Map[String, Any => Any] = {
    Map.empty
  }
}
