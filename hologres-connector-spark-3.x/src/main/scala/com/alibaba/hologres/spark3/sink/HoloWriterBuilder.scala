package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.{HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import com.alibaba.hologres.spark3.sink.copy.HoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

/** HoloWriterBuilder. */
class HoloWriterBuilder(sourceOptions: Map[String, String],
                        schema: StructType) extends WriteBuilder with SupportsOverwrite {
  var is_overwrite: Boolean = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    is_overwrite = true
    this
  }

  override def build(): Write = new Write() {
    override def toBatch: BatchWrite = {
      new HoloBatchWriter(sourceOptions, schema, is_overwrite)
    }
  }
}

/** HoloBatchWriter: To create HoloWriterFactory. */
class HoloBatchWriter(
                       sourceOptions: Map[String, String],
                       sparkSchema: StructType,
                       is_overwrite: Boolean) extends BatchWrite {
  private val logger = LoggerFactory.getLogger(getClass)
  val hologresConfigs: HologresConfigs = new HologresConfigs(sourceOptions)
  private var partitionInfo: (String, String) = _
  logger.info("HoloBatchWriter begin: " + LocalDateTime.now())

  if (is_overwrite) {
    hologresConfigs.tempTableForOverwrite = JDBCUtil.generateTempTableNameForOverwrite(hologresConfigs)
    JDBCUtil.createTempTableForOverWrite(hologresConfigs)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info("HoloBatchWriter commit: " + LocalDateTime.now())
    if (is_overwrite) {
      if (partitionInfo.eq(null)) {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs)
      } else {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs, partitionInfo._1, partitionInfo._2)
      }
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn("HoloBatchWriter abort: " + LocalDateTime.now())
    if (is_overwrite) {
      JDBCUtil.deleteTempTableForOverWrite(hologresConfigs)
    }
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): HoloWriterFactory = {
    val numPartitions = physicalWriteInfo.numPartitions()
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

      val shardCount = getShardCount(holoClient, holoSchema)

      // 用户设置bulkLoad或者发现表无主键且实例版本支持无主键并发COPY，都走bulkLoad
      val supportBulkLoad = holoSchema.getPrimaryKeys.length == 0 && holoVersion.compareTo(new HoloVersion(2, 1, 0)) > 0
      if (hologresConfigs.bulkLoad || supportBulkLoad) {
        hologresConfigs.bulkLoad = true
        logger.info("bulk load mode, have primary keys: {}, holoVersion {}", holoSchema.getPrimaryKeys.length == 0, holoVersion)
      }

      val supportCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0
      if (!supportCopy) {
        logger.warn("The hologres instance version is {}, but only instances greater than 1.3.24 support copy write mode", holoVersion)
        hologresConfigs.copy_write_mode = false
      } else {
        // 尝试直连，无法直连则各个tasks内的copy writer不需要进行尝试
        hologresConfigs.copy_write_direct_connect = JDBCUtil.couldDirectConnect(hologresConfigs)
      }
      HoloWriterFactory(hologresConfigs, sparkSchema, holoSchema, numPartitions, shardCount)
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
                              holoSchema: TableSchema,
                              numPartitions: Int,
                              shardCount: Int) extends DataWriterFactory {
  private def getTargetShardList(partitionId: Int): String = {
    val targetShardList = ListBuffer[Int]()
    val maxI = (shardCount - partitionId) / numPartitions

    for (i <- 0 to maxI) {
      val p = i * numPartitions + partitionId
      targetShardList += p
    }
    targetShardList.mkString(",")
  }

  override def createWriter(
                             partitionId: Int,
                             taskId: Long): DataWriter[InternalRow] = {
    if (hologresConfigs.copy_write_mode) {
      if (hologresConfigs.enable_target_shards) {
        new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, getTargetShardList(partitionId))
      } else {
        new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema)
      }
    } else {
      new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    }
  }
}
