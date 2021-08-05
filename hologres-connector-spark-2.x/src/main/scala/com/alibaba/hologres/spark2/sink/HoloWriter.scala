package com.alibaba.hologres.spark2.sink

import com.alibaba.hologres.spark.sink.HoloClientInstance
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/** HoloWriter: To create HoloWriterFactory or HoloStreamWriterFactory. */
class HoloWriter(
                  table: String,
                  holoOptions: Map[String, String],
                  schema: Option[StructType],
                  streamMode: (Boolean, OutputMode)) extends DataSourceWriter with StreamWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  if (streamMode._2 == OutputMode.Complete()) {
    HoloClientInstance.setHoloOptions(holoOptions)
    HoloClientInstance.setOutputMode(streamMode._2)
    logger.debug("HoloWriter create HoloClientInstance : " + HoloClientInstance.client.toString)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.debug("HoloWriter commit")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.debug("HoloWriter abort")
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logger.debug("HoloWriter Stream commit")
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logger.debug("HoloWriter Stream abort")
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    if (streamMode._1) {
      HoloStreamWriterFactory(table, holoOptions, schema)
    } else {
      HoloWriterFactory(table, holoOptions, schema)
    }
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
    new HoloDataWriter(table, holoOptions, schema, null)
  }
}

/** HoloStreamWriterFactory. */
case class HoloStreamWriterFactory(
                                    table: String,
                                    holoOptions: Map[String, String],
                                    schema: Option[StructType]) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
                                 partitionId: Int,
                                 taskId: Long,
                                 epochId: Long): DataWriter[InternalRow] = {
    // StreamWriter's Complete OutputMode always create new writer, if not reuse holo-client, it will exceed the number of connections.
    if (HoloClientInstance.getOutputMode == OutputMode.Complete()) {
      new HoloDataWriter(table, holoOptions, schema, HoloClientInstance.client)
    } else {
      new HoloDataWriter(table, holoOptions, schema, null)
    }
  }
}
