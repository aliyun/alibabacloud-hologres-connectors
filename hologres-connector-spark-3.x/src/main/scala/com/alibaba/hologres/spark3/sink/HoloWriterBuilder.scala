package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.{HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.BaseSourceProvider
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import com.alibaba.hologres.spark3.sink.copy.HoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.IOException

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
                       sparkSchema: Option[StructType]) extends BatchWrite {
  private val logger = LoggerFactory.getLogger(getClass)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): HoloWriterFactory = {
    val hologresConfigs: HologresConfigs = new HologresConfigs(sourceOptions)
    val holoClient: HoloClient = new BaseSourceProvider().getOrCreateHoloClient(hologresConfigs)
    val holoSchema: TableSchema = holoClient.getTableSchema(TableName.valueOf(table))
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
    if (holoClient != null) {
      holoClient.close()
    }
    HoloWriterFactory(hologresConfigs, sparkSchema, holoSchema)
  }
}

/** HoloWriterFactory. */
case class HoloWriterFactory(
                              hologresConfigs: HologresConfigs,
                              sparkSchema: Option[StructType],
                              holoSchema: TableSchema) extends DataWriterFactory {
  override def createWriter(
                             partitionId: Int,
                             taskId: Long): DataWriter[InternalRow] = {
    if (hologresConfigs.copy_write_mode) {
      new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema)
    } else {
      new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    }
  }
}
