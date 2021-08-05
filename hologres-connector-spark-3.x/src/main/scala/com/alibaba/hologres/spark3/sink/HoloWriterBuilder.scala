package com.alibaba.hologres.spark3.sink

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** HoloWriterBuilder. */
class HoloWriterBuilder(table: String,
                        sourceOptions: Map[String, String],
                        schema: Option[StructType]) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = {
    new HoloBatchWriter(table, sourceOptions, schema)
  }
}

/** HoloBatchWriter: To create HoloWriterFactory. */
class HoloBatchWriter(
                       table: String,
                       sourceOptions: Map[String, String],
                       schema: Option[StructType]) extends BatchWrite {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): HoloWriterFactory = {
    HoloWriterFactory(table, sourceOptions, schema)
  }
}

/** HoloWriterFactory. */
case class HoloWriterFactory(
                              table: String,
                              sourceOptions: Map[String, String],
                              schema: Option[StructType]) extends DataWriterFactory {
  override def createWriter(
                             partitionId: Int,
                             taskId: Long): DataWriter[InternalRow] = {
    new HoloDataWriter(table, sourceOptions, schema, null)
  }
}
