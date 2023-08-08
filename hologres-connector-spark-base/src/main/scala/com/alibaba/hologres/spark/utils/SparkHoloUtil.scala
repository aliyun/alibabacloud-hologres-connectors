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

  // 通过holo表生成spark schema，如果传入了spark的DDL则根据DDL生成并检查schema是否匹配
  def checkSparkTableSchema(hologresConfigs: HologresConfigs, sparkSchema: StructType): Unit = {
    @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      val holoSchema: TableSchema = holoClient.getTableSchema(hologresConfigs.table)
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

    } finally {
      holoClient.close()
    }
  }

  def getSparkTableSchema(hologresConfigs: HologresConfigs): StructType = {
    @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      val holoSchema: TableSchema = holoClient.getTableSchema(hologresConfigs.table)
      val fields = ArrayBuffer[StructField]()
      holoSchema.getColumnSchema.foreach(column => {
        fields += StructField(column.getName, getSparkDataType(column))
      })

      StructType(fields)
    } finally {
      holoClient.close()
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
