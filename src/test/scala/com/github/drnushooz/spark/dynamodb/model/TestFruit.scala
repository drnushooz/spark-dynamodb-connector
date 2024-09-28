package com.github.drnushooz.spark.dynamodb.model

import com.github.drnushooz.spark.dynamodb.attribute

case class TestFruit(@attribute("name") primaryKey: String, color: String, weightKg: Double)
