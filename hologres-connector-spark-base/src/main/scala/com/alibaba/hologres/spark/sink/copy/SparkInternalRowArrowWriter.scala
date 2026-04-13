package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.copy.in.arrow.{AbstractArrowWriter, ArrowVectorCreatorUtil}
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import com.alibaba.hologres.spark.sink.FieldWriter
import org.apache.spark.sql.catalyst.InternalRow

import java.util

class SparkInternalRowArrowWriter(schema : TableSchema,
                                  columns : java.util.List[String],
                                  batchSize : Int,
                                  fieldWriters: Array[FieldWriter])  extends AbstractArrowWriter[InternalRow](schema, columns, batchSize) {

  override def fillVectorSchemaRoot(root: VectorSchemaRoot, rows: util.List[InternalRow]): Unit = {
    if (rows == null || rows.isEmpty) {
      root.setRowCount(0)
      return
    }

    root.clear();
    val fieldVectors: util.List[FieldVector] = root.getFieldVectors
    for(i <- 0 until fieldVectors.size()) {
      fieldVectors.get(i).allocateNew()
    }

    if (fieldVectors.size != columns.size) throw new IllegalArgumentException("Column count mismatch")

    // Step 3: Fill each vector// Step 3: Fill each vector
    for (i <- 0 until rows.size()) {
      val row = rows.get(i)
      for (j <- 0 until columns.size) {
        val columnName = columns.get(j)
        val index = schema.getColumnIndex(columnName)
        val column = schema.getColumn(index)
        val vector = fieldVectors.get(j)
        val columnVectorCreator = ArrowVectorCreatorUtil.createColumnVectorCreator(vector, column)
        val value = if (!row.isNullAt(j)) fieldWriters.apply(j).writeValue(row, j) else null
        columnVectorCreator.set(i, value)
      }
    }

    // Step 4: Set row count
    root.setRowCount(rows.size())
  }
}
