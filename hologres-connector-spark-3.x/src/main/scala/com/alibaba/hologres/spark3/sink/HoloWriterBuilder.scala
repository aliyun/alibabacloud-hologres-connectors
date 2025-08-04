package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.{TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}
import com.alibaba.hologres.spark3.sink.copy.HoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

/** HoloWriterBuilder. */
class HoloWriterBuilder(hologresConfigs: HologresConfigs,
                        schema: StructType) extends WriteBuilder with SupportsOverwrite {
  var is_overwrite: Boolean = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    is_overwrite = true
    this
  }

  override def build(): Write = new Write() {
    override def toBatch: BatchWrite = {
      new HoloBatchWriter(hologresConfigs, schema, is_overwrite)
    }
  }
}

/** HoloBatchWriter: To create HoloWriterFactory. */
class HoloBatchWriter(
                       hologresConfigs: HologresConfigs,
                       sparkSchema: StructType,
                       is_overwrite: Boolean) extends BatchWrite {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)

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

      val shardCount = getShardCount(holoClient, holoSchema)
      logger.info(s"write mode ${hologresConfigs.writeMode.toString}")
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
    if ("insert" != hologresConfigs.writeMode) {
      if (hologresConfigs.reshuffleByHoloDistributionKey) {
        new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, getTargetShardList(partitionId), taskId.toString)
      } else {
        new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, taskId = taskId.toString)
      }
    } else {
      new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    }
  }
}
