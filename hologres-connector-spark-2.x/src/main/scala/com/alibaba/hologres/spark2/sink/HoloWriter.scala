package com.alibaba.hologres.spark2.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.{HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import com.alibaba.hologres.spark2.sink.copy.HoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.IOException

/** HoloWriter: To create HoloWriterFactory or HoloStreamWriterFactory. */
class HoloWriter(
                  holoOptions: Map[String, String],
                  sparkSchema: StructType) extends DataSourceWriter with StreamWriter {
  private val logger = LoggerFactory.getLogger(getClass)

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
    val hologresConfigs: HologresConfigs = new HologresConfigs(holoOptions)
    val holoClient: HoloClient = new HoloClient(hologresConfigs.holoConfig)
    var holoSchema: TableSchema = null
    try {
      holoSchema = holoClient.getTableSchema(TableName.valueOf(hologresConfigs.table))
      var holoVersion: HoloVersion = null

      try holoVersion = holoClient.sql[HoloVersion](getHoloVersion).get()
      catch {
        case e: Exception =>
          throw new IOException("Failed to get holo version", e)
      }
      val supportCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0
      if (!supportCopy) {
        logger.warn("The hologres instance version is {}, but only instances greater than 1.3.24 support copy write mode", holoVersion)
        hologresConfigs.copy_write_mode = false
      } else {
        // 尝试直连，无法直连则各个tasks内的copy writer不需要进行尝试
        hologresConfigs.copy_write_direct_connect = JDBCUtil.couldDirectConnect(hologresConfigs)
      }
    } finally {
      if (holoClient != null) {
        holoClient.close()
      }
    }
    HoloWriterFactory(hologresConfigs, sparkSchema, holoSchema)
  }
}

/** HoloWriterFactory. */
case class HoloWriterFactory(
                              hologresConfigs: HologresConfigs,
                              sparkSchema: StructType,
                              holoSchema: TableSchema) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
                                 partitionId: Int,
                                 taskId: Long,
                                 epochId: Long): DataWriter[InternalRow] = {
    if (hologresConfigs.copy_write_mode) {
      new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema)
    } else {
      new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    }
  }
}

