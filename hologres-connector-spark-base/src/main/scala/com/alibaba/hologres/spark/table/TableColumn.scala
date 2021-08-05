package com.alibaba.hologres.spark.table

import com.alibaba.hologres.client.model.Record
import com.alibaba.hologres.spark.exception.SparkHoloException
import org.apache.spark.sql.types.StructType
import org.postgresql.model.TableSchema

/** TableColumn statistics of a schema. */
class TableColumn(sparkSchema: StructType, holoSchema: TableSchema) {
  private val columnCounts = sparkSchema.size
  private val columns = new Array[Column](columnCounts)
  private val columnIdToHoloId = new Array[Int](columnCounts)

  private val record = Record.build(holoSchema)

  var idx = 0
  sparkSchema.foreach(field => {
    val column = new Column(field.name, field.dataType, field.nullable)
    addColumn(column, idx)
    idx = idx + 1
  })

  def addColumn(column: Column, idx: Int): Unit = {
    if (column == null) {
      throw new IllegalArgumentException("Column is null.")
    }
    else {
      this.columns(idx) = column
      if (record.getSchema.getColumnIndex(column.getColumnName) == null) {
        throw new SparkHoloException("Column name <" + column.getColumnName + "> not exist in hologres")
      }
      this.columnIdToHoloId(idx) = record.getSchema.getColumnIndex(column.getColumnName)
    }
  }

  def getColumn(idx: Int): Column = {
    if (idx >= 0 && idx < this.columns.length) {
      this.columns(idx)
    }
    else {
      throw new IllegalArgumentException("idx out of range")
    }
  }

  def getColumns: Array[Column] = {
    this.columns
  }

  def getColumnIdToHoloId: Array[Int] = {
    this.columnIdToHoloId
  }

  def getColumnCounts: Int = {
    this.columnCounts
  }
}
