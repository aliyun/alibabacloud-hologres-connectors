package com.alibaba.hologres.spark2.sink

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** HoloWriter: To create HoloWriterFactory. */
class HoloWriter(
                  table: String,
                  holoOptions: Map[String, String],
                  schema: Option[StructType]) extends DataSourceWriter {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createWriterFactory(): HoloWriterFactory = {
    HoloWriterFactory(table, holoOptions, schema)
  }
}

/** HoloWriterFactory. */
case class HoloWriterFactory(
                              table: String,
                              holoOptions: Map[String, String],
                              schema: Option[StructType]) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
                                 partitionId: Int,
                                 taskId: Long,
                                 epochId: Long): DataWriter[InternalRow] = {
    new HoloDataWriter(table, holoOptions, schema)
  }
}
