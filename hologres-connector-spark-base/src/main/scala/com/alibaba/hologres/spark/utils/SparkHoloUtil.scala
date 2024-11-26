package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.{Column, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.Types
import scala.collection.mutable.ArrayBuffer

object SparkHoloUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  // 检查schema是否匹配, 其实是根据holo表或者查询的query生成一个默认的spark schema, 和传入的spark schema进行比较,
  // 传入的spark schema必须是holo表或者查询的query生成的spark schema的一部分
  def checkSparkTableSchema(hologresConfigs: HologresConfigs, sparkSchema: StructType, mockHoloSchemaForQuery: TableSchema = null): Unit = {
    var holoSchema: TableSchema = null
    if (hologresConfigs.isTableSource && holoSchema == null) {
      @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
      try {
        holoSchema = holoClient.getTableSchema(hologresConfigs.table)
      } finally {
        holoClient.close()
      }
    } else {
      holoSchema = mockHoloSchemaForQuery
    }
    logger.info("spark schema: " + sparkSchema.toDDL)
    logger.info("holo schema: " + holoSchema)
    sparkSchema.fields.foreach(column => {
      if (holoSchema.getColumnIndex(column.name) == null) {
        throw new IllegalArgumentException(String.format("column %s does not exist in hologres table %s", column.name, holoSchema.getTableName))
      }
      val holoColumn = holoSchema.getColumn(holoSchema.getColumnIndex(column.name))
      if (column.dataType != getSparkDataType(holoColumn)) {
        throw new IllegalArgumentException(String.format("column %s in hologres table %s type does not match: spark type: %s, hologres type: %s",
          column.name, holoSchema.getTableName, column.dataType, holoColumn.getTypeName))
      }
    })
  }

  // 如果未传入spark的DDL，则根据holo表或者查询的query推断一个默认的spark schema
  def inferSparkTableSchema(hologresConfigs: HologresConfigs, mockHoloSchemaForQuery: TableSchema = null): StructType = {
    var holoSchema: TableSchema = null
    if (hologresConfigs.isTableSource && holoSchema == null) {
      @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
      try {
        holoSchema = holoClient.getTableSchema(hologresConfigs.table)
      } finally {
        holoClient.close()
      }
    } else {
      holoSchema = mockHoloSchemaForQuery
    }
    val fields = ArrayBuffer[StructField]()
    holoSchema.getColumnSchema.foreach(column => {
      fields += StructField(column.getName, getSparkDataType(column))
    })

    StructType(fields)
  }

  def mockHoloSchemaForQuery(hologresConfigs: HologresConfigs): TableSchema = {
    @transient val conn = JDBCUtil.createConnection(hologresConfigs)
    try {
      var holoSchema: TableSchema = null
      val metadata = conn.prepareStatement(hologresConfigs.query).getMetaData
      val columnCount = metadata.getColumnCount
      // 如果通过query查询holo,这里mock一个holo schema
      val mockSchemaBuilder: TableSchema.Builder = new TableSchema.Builder()
      for (i <- 1 to columnCount) {
        val column: Column = new Column()
        column.setName(metadata.getColumnName(i))
        column.setType(metadata.getColumnType(i))
        column.setTypeName(metadata.getColumnTypeName(i))
        column.setPrecision(metadata.getPrecision(i))
        column.setScale(metadata.getScale(i))
        column.setArrayType(column.getTypeName.startsWith("_"))
        if (column.isArrayType) {
          column.setArrayElementType(column.getTypeName.replaceFirst("_", ""))
        }
        mockSchemaBuilder.addColumn(column)
      }
      holoSchema = mockSchemaBuilder.build()
      holoSchema.calculateProperties()
      holoSchema
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(String.format("could not get metadata for query [%s]", hologresConfigs.query), e)
    } finally {
      conn.close()
    }
  }

  def getSparkDataType(column: Column): DataType = {
    column.getType match {
      case Types.SMALLINT | Types.TINYINT => DataTypes.ShortType
      case Types.INTEGER => DataTypes.IntegerType
      case Types.BIGINT => DataTypes.LongType
      case Types.REAL | Types.FLOAT => DataTypes.FloatType
      case Types.DOUBLE => DataTypes.DoubleType
      case Types.BOOLEAN | Types.BIT => DataTypes.BooleanType
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => DataTypes.TimestampType
      case Types.BINARY => DataTypes.BinaryType
      case Types.DATE => DataTypes.DateType
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR => DataTypes.StringType
      case Types.NUMERIC | Types.DECIMAL => DecimalType(column.getPrecision, column.getScale)
      case Types.ARRAY =>
        column.getTypeName match {
          case "_int4" => ArrayType(DataTypes.IntegerType)
          case "_int8" => ArrayType(DataTypes.LongType)
          case "_float4" => ArrayType(DataTypes.FloatType)
          case "_float8" => ArrayType(DataTypes.DoubleType)
          case "_bool" => ArrayType(DataTypes.BooleanType)
          case "_text" => ArrayType(DataTypes.StringType)
        }
      case _ =>
        column.getTypeName match {
          case "roaringbitmap" => DataTypes.BinaryType
          case "json" | "jsonb" => DataTypes.StringType
          case _ => throw new IllegalArgumentException(String.format("Column type %s does not supported now",
            column.getTypeName))
        }
    }
  }
}
