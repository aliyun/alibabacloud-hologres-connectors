package com.alibaba.hologres.spark.table

import com.alibaba.hologres.spark.table.ColumnType.ColumnType
import com.alibaba.hologres.spark.utils.DataTypeUtil.{getArrayType, getDecimalParameter}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType}

class Column(name: String, dataType: DataType, nullable: Boolean) {
  private var precision = 38
  private var scale = 18
  private val allowNull = nullable

  private val columnName = name
  private val columnType = dataType match {
    case DataTypes.IntegerType => ColumnType.INT
    case DataTypes.LongType => ColumnType.BIGINT
    case DataTypes.FloatType => ColumnType.FLOAT
    case DataTypes.DoubleType => ColumnType.DOUBLE
    case DataTypes.BooleanType => ColumnType.BOOLEAN
    case DataTypes.TimestampType => ColumnType.TIMESTAMP
    case DataTypes.BinaryType => ColumnType.BYTEA
    case DataTypes.DateType => ColumnType.DATE
    case _: DecimalType =>
      val decimalParameter = getDecimalParameter(dataType.toString)
      precision = decimalParameter(0)
      scale = decimalParameter(1)
      ColumnType.DECIMAL
    case _: ArrayType =>
      getArrayType(dataType.toString) match {
        case "IntegerType" => ColumnType.INTA
        case "LongType" => ColumnType.BIGINTA
        case "FloatType" => ColumnType.FLOATA
        case "DoubleType" => ColumnType.DOUBLEA
        case "BooleanType" => ColumnType.BOOLEANA
        case "StringType" => ColumnType.TEXTA
      }
    case _ => ColumnType.TEXT
  }

  def getColumnName: String = {
    this.columnName
  }

  def getColumnType: ColumnType = {
    this.columnType
  }

  def isAllowNull: Boolean = {
    this.allowNull
  }

  def getPrecision: Int = {
    this.precision
  }

  def getScale: Int = {
    this.scale
  }
}
