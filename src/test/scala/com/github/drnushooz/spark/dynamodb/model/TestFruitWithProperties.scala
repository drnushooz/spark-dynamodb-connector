package com.github.drnushooz.spark.dynamodb.model

case class TestFruitProperties(freshness: String, eco: Boolean, price: Double)

case class TestFruitWithProperties(name: String, color: String, weight: Double, properties: TestFruitProperties)
