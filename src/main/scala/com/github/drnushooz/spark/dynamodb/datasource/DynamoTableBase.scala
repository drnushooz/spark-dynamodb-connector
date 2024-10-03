package com.github.drnushooz.spark.dynamodb.datasource

import com.github.drnushooz.spark.dynamodb.connector.DynamoTableConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type

import java.util
import scala.collection.compat.immutable._
import scala.jdk.CollectionConverters._

class DynamoTableBase(options: CaseInsensitiveStringMap, userSchema: Option[StructType] = None)
    extends Table
    with SupportsRead
    with SupportsWrite {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val dynamoConnector: DynamoTableConnector = {
    val indexName = Option(options.get("indexName"))
    val defaultParallelism =
      Option(options.get("defaultParallelism")).map(_.toInt).getOrElse(getDefaultParallelism)
    val optionsMap = options.asScala.toMap

    DynamoTableConnector(name(), defaultParallelism, optionsMap, indexName)
  }

  override def name(): String = options.get("tableName")

  override def schema(): StructType = userSchema.getOrElse(inferSchema())

  override def columns(): Array[Column] = CatalogV2UtilProxy.structTypeToV2Columns(schema())

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new DynamoScanBuilder(dynamoConnector, schema())
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val parameters = info.options.asScala.toMap
    dynamoConnector match {
      case tableConnector: DynamoTableConnector =>
        DynamoWriteBuilder(tableConnector, parameters, info.schema())
      case _ =>
        throw new RuntimeException("Unable to write to a GSI, please omit `indexName` option.")
    }
  }

  private def getDefaultParallelism: Int =
    SparkSession.getActiveSession match {
      case Some(spark) => spark.sparkContext.defaultParallelism
      case None =>
        logger.warn(
          "Unable to read defaultParallelism from SparkSession." +
            " Parallelism will be 1 unless overwritten with option `defaultParallelism`")
        1
    }

  private def inferSchema(): StructType = {
    val inferenceItems =
      if (dynamoConnector.nonEmpty && options.getBoolean("inferSchema", true)) {
        val scanResponses = dynamoConnector.scan(Seq.empty, Seq.empty).iterator()
        if (scanResponses.hasNext)
          scanResponses.next.items.asScala
        else
          Seq.empty
      } else
        Seq.empty

    val typeMapping = attributeTypeMapping(ArraySeq.unsafeWrapArray(inferenceItems.toArray))
    val typeSeq = typeMapping.map { case (name, sparkType) => StructField(name, sparkType) }.toSeq

    if (typeSeq.size > 100) {
      throw new RuntimeException("Schema inference not possible, too many attributes in table.")
    }

    StructType(typeSeq)
  }

  protected def inferType(value: AttributeValue): DataType = value.`type` match {
    case Type.S => StringType
    case Type.N =>
      val nValue = BigDecimal(value.n)
      if (nValue.scale == 0) {
        if (nValue.precision < 10) IntegerType
        else if (nValue.precision < 19) LongType
        else DataTypes.createDecimalType(nValue.precision, nValue.scale)
      } else
        DoubleType
    case Type.B => BinaryType
    case Type.BOOL => BooleanType
    case Type.NUL => NullType
    case Type.M =>
      val mValue = value.m.asScala
      val mapFields = mValue.map { case (fieldName, fieldValue) =>
        StructField(fieldName, inferType(fieldValue))
      }
      StructType(mapFields.toSeq)
    case Type.L =>
      val vList = value.l
      if (vList.isEmpty) {
        ArrayType(StringType)
      } else {
        ArrayType(inferType(vList.get(0)))
      }
    case Type.SS => ArrayType(StringType)
    case Type.NS => ArrayType(DoubleType)
    case Type.BS => ArrayType(BinaryType)
    case _ => StringType
  }

  def attributeTypeMapping(inferenceItems: ArraySeq[util.Map[String, AttributeValue]]): Map[String, DataType] = {
    Map.empty
  }
}
