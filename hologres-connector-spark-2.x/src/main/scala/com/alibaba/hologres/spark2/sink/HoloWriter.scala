package com.alibaba.hologres.spark2.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.{HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import com.alibaba.hologres.spark2.sink.copy.HoloDataCopyWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.LocalDateTime
import java.util.Random
import scala.collection.mutable.ListBuffer

/** HoloWriter: To create HoloWriterFactory or HoloStreamWriterFactory. */
class HoloWriter(
                  holoOptions: Map[String, String],
                  sparkSchema: StructType,
                  mode: SaveMode) extends DataSourceWriter with StreamWriter {
  private val logger = LoggerFactory.getLogger(getClass)
  logger.info("HoloWriter begin: " + LocalDateTime.now())
  val hologresConfigs: HologresConfigs = new HologresConfigs(holoOptions)
  var is_overwrite: Boolean = mode == SaveMode.Overwrite
  private var partitionInfo: (String, String) = _

  if (is_overwrite) {
    hologresConfigs.tempTableForOverwrite = JDBCUtil.generateTempTableNameForOverwrite(hologresConfigs)
    JDBCUtil.createTempTableForOverWrite(hologresConfigs)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info("HoloWriter commit: " + LocalDateTime.now())
    if (is_overwrite) {
      if (partitionInfo.eq(null)) {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs)
      } else {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs, partitionInfo._1, partitionInfo._2)
      }
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn("HoloWriter abort: " + LocalDateTime.now())
    if (is_overwrite) {
      JDBCUtil.deleteTempTableForOverWrite(hologresConfigs)
    }
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logger.info("HoloWriter Stream commit")
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logger.info("HoloWriter Stream abort")
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    val holoClient: HoloClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      var holoSchema = holoClient.getTableSchema(TableName.valueOf(hologresConfigs.table))
      if (holoSchema.isPartitionParentTable && is_overwrite) {
        throw new IOException("Partition parent table can not be insert overwrite now.")
      }
      partitionInfo = JDBCUtil.getChildTablePartitionInfo(hologresConfigs)

      if (is_overwrite) {
        // insert overwrite 会先写在一张临时表中，写入成功时替换原表。
        holoSchema = holoClient.getTableSchema(TableName.valueOf(hologresConfigs.tempTableForOverwrite))
      }

      var holoVersion: HoloVersion = null
      try holoVersion = holoClient.sql[HoloVersion](getHoloVersion).get()
      catch {
        case e: Exception =>
          throw new IOException("Failed to get holo version", e)
      }

      // 用户设置bulkLoad或者发现表无主键且实例版本支持无主键并发COPY，都走bulkLoad
      val supportBulkLoad = holoSchema.getPrimaryKeys.length == 0 && holoVersion.compareTo(new HoloVersion(2, 1, 0)) > 0
      if (hologresConfigs.copyMode == CopyMode.BULK_LOAD || supportBulkLoad) {
        hologresConfigs.copyMode = CopyMode.BULK_LOAD
        logger.info("bulk load mode, have primary keys: {}, holoVersion {}", holoSchema.getPrimaryKeys.length == 0, holoVersion)
      }

      val supportCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0
      if (!supportCopy) {
        logger.warn("The hologres instance version is {}, but only instances greater than 1.3.24 support copy write mode", holoVersion)
        hologresConfigs.copyMode = null
      } else {
        // 尝试直连，无法直连则各个tasks内的copy writer不需要进行尝试
        hologresConfigs.copy_write_direct_connect = JDBCUtil.couldDirectConnect(hologresConfigs)
      }
      HoloWriterFactory(hologresConfigs, sparkSchema, holoSchema)
    } finally {
      if (holoClient != null) {
        holoClient.close()
      }
    }
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
    if (hologresConfigs.copyMode != null) {
      new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema)
    } else {
      new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    }
  }
}

