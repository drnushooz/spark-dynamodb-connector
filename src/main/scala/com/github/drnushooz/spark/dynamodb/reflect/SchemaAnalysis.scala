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
package com.github.drnushooz.spark.dynamodb.reflect

import com.github.drnushooz.spark.dynamodb.attribute
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[dynamodb] object SchemaAnalysis {
  def apply[T <: Product: ClassTag: TypeTag]: (StructType, Map[String, String]) = {
    val mirror = runtimeMirror(getClass.getClassLoader)

    val classObj = scala.reflect.classTag[T].runtimeClass
    val classSymbol = mirror.classSymbol(classObj)

    val params = classSymbol.primaryConstructor.typeSignature.paramLists.head
    val (sparkFields, aliasMap) =
      params.foldLeft((List.empty[StructField], Map.empty[String, String])) { case ((list, map), field) =>
        val sparkType = ScalaReflection.schemaFor(field.typeSignature).dataType
        val attrName = field.annotations.collectFirst {
          case ann: AnnotationApi if ann.tree.tpe =:= typeOf[attribute] =>
            ann.tree.children.tail.collectFirst { case Literal(Constant(name: String)) =>
              name
            }
        }.flatten

        attrName
          .map { n =>
            val sparkField = StructField(n, sparkType, nullable = true)
            (list :+ sparkField, map + (n -> field.name.toString))
          }
          .getOrElse {
            val sparkField = StructField(field.name.toString, sparkType, nullable = true)
            (list :+ sparkField, map)
          }
      }
    (StructType(sparkFields), aliasMap)
  }
}
