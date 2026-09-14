package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.copy.in.arrow.{AbstractArrowWriter, ArrowVectorCreatorUtil}
import com.alibaba.hologres.client.copy.in.arrow.creator.AbstractArrowVectorCreator
import com.alibaba.hologres.client.model.{Column, TableSchema}
import com.alibaba.hologres.spark.sink.FieldWriter
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow

import java.util

/** Arrow writer for Spark InternalRow. */
class SparkInternalRowArrowWriter(schema: TableSchema,
                                  columns: java.util.List[String],
                                  batchSize: Int,
                                  fieldWriters: Array[FieldWriter],
                                  enableCompression: Boolean = false)
  extends AbstractArrowWriter[InternalRow](schema, columns, batchSize, enableCompression) {

  private val columnCount: Int = columns.size
  private val holoColumns: Array[Column] = new Array[Column](columnCount)

  for (i <- 0 until columnCount) {
    holoColumns(i) = schema.getColumn(schema.getColumnIndex(columns.get(i)))
  }

  @transient private var boundRoot: VectorSchemaRoot = _
  @transient private var vectorCreators: Array[AbstractArrowVectorCreator] = _

  private def ensureVectorCreators(root: VectorSchemaRoot): Unit = {
    if (root ne boundRoot) {
      val fieldVectors: util.List[FieldVector] = root.getFieldVectors
      if (fieldVectors.size != columnCount) {
        throw new IllegalArgumentException(
          s"Column count mismatch, vectors=${fieldVectors.size}, columns=$columnCount")
      }

      vectorCreators = new Array[AbstractArrowVectorCreator](columnCount)
      for (i <- 0 until columnCount) {
        vectorCreators(i) = ArrowVectorCreatorUtil.createColumnVectorCreator(
          fieldVectors.get(i), holoColumns(i))
      }
      boundRoot = root
    }
  }

  override def fillVectorSchemaRoot(root: VectorSchemaRoot, rows: util.List[InternalRow]): Unit = {
    if (rows == null || rows.isEmpty) {
      root.setRowCount(0)
      return
    }

    root.clear()
    val fieldVectors: util.List[FieldVector] = root.getFieldVectors
    for (i <- 0 until fieldVectors.size()) {
      fieldVectors.get(i).allocateNew()
    }

    ensureVectorCreators(root)

    val rowCount = rows.size()
    for (i <- 0 until rowCount) {
      val row = rows.get(i)
      for (j <- 0 until columnCount) {
        val value = if (!row.isNullAt(j)) fieldWriters(j).writeValue(row, j) else null
        vectorCreators(j).set(i, value)
      }
    }

    root.setRowCount(rowCount)
  }
}
