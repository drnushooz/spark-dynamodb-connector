package com.github.drnushooz.spark.dynamodb.connector

import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.connector.expressions.filter._
import org.apache.spark.sql.types._

private[dynamodb] object PredicatePushdown {
  def apply(predicates: Seq[Predicate]): String = {
    predicates.map(buildCondition).mkString(" AND ")
  }

  def acceptPredicates(predicates: Array[Predicate]): (Array[Predicate], Array[Predicate]) = {
    predicates.partition(checkPredicate)
  }

  private def checkPredicate(predicate: Predicate): Boolean = predicate.name match {
    case "ENDS_WITH" => false
    case "AND" =>
      val andPredicate = predicate.asInstanceOf[And]
      checkPredicate(andPredicate.left) && checkPredicate(andPredicate.right)
    case "OR" =>
      val orPredicate = predicate.asInstanceOf[Or]
      checkPredicate(orPredicate.left) && checkPredicate(orPredicate.right)
    case "NOT" =>
      val notPredicate = predicate.asInstanceOf[Not]
      checkPredicate(notPredicate.child)
    case _ => true
  }

  private def buildCondition(predicate: Predicate): String = {
    predicate.name match {
      case "=" | ">" | ">=" | "<" | "<=" | "<>" =>
        val children = predicate.children
        s"${children(0)} ${predicate.name} ${children(1)}"

      case "<=>" =>
        val children = predicate.children
        s"${children(0)} = ${children(1)} OR (${children(0)} = NULL AND ${children(1)} = NULL)"

      case "IN" =>
        val children = predicate.children
        val attribute = children(0)
        val lit = children(1).asInstanceOf[Literal[_]]
        val valueList = children.slice(1, children.length).map(_.asInstanceOf[Literal[_]].value)
        val valueExp = lit.dataType match {
          case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DecimalType() =>
            s"""[${valueList.toSet.mkString(",")}]"""
          case _ =>
            s"[${valueList.toSet.mkString("'", "','", "'")}]"
        }
        s"$attribute IN $valueExp"

      case "IS_NULL" =>
        val attribute = predicate.children()(0)
        s"$attribute IS NULL"

      case "IS_NOT_NULL" =>
        val attribute = predicate.children()(0)
        s"$attribute IS NOT NULL"

      case "STARTS_WITH" =>
        val children = predicate.children
        val attribute = children(0)
        val lv = children(1).asInstanceOf[Literal[_]].value
        s"""BEGINS_WITH("$attribute", '$lv')"""

      case "CONTAINS" =>
        val children = predicate.children
        val attribute = children(0).asInstanceOf[Literal[_]].value
        val litVal = children(1).asInstanceOf[Literal[_]].value
        s"""CONTAINS("$attribute", '$litVal')"""

      case "AND" =>
        val andPredicate = predicate.asInstanceOf[And]
        s"${buildCondition(andPredicate.left)} AND ${buildCondition(andPredicate.right)}"

      case "OR" =>
        val orPredicate = predicate.asInstanceOf[Or]
        s"${buildCondition(orPredicate.left)} OR ${buildCondition(orPredicate.right)}"

      case "NOT" =>
        val notPredicate = predicate.asInstanceOf[Not]
        s"NOT ${buildCondition(notPredicate.child)}"

      case _ =>
        throw new UnsupportedOperationException(s"Predicate '${predicate.name}' is not supported by DynamoDB")
    }
  }
}
