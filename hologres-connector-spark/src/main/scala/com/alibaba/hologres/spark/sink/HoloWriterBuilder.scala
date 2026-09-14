package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.{TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.copy.StageWriterCommitMessage
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper, StagePartitionUtil, TargetShardUtil}
import com.alibaba.hologres.spark.sink.copy.{HoloDataCopyWriter, HoloDataStageWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.time.LocalDateTime
import scala.collection.JavaConverters.seqAsJavaListConverter

/** HoloWriterBuilder. */
class HoloWriterBuilder(hologresConfigs: HologresConfigs,
                        schema: StructType) extends WriteBuilder with SupportsOverwrite with SupportsDynamicOverwrite {
  var is_overwrite: Boolean = false
  // Spark SQL `INSERT OVERWRITE t PARTITION (col = value)` 静态分区spec会被翻译为EqualTo filters传入
  var overwriteFilters: Array[Filter] = Array.empty

  // 动态分区覆盖: 覆盖数据中实际出现的分区, 分区值由writer自动采集, 仅stage模式支持
  var dynamicPartitionOverwrite: Boolean = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    is_overwrite = true
    overwriteFilters = filters
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    is_overwrite = true
    dynamicPartitionOverwrite = true
    this
  }

  override def build(): Write = new Write() {
    override def toBatch: BatchWrite = {
      new HoloBatchWriter(hologresConfigs, schema, is_overwrite, overwriteFilters, dynamicPartitionOverwrite)
    }
  }
}

/** HoloBatchWriter: To create HoloWriterFactory. */
class HoloBatchWriter(
                       hologresConfigs: HologresConfigs,
                       sparkSchema: StructType,
                       is_overwrite: Boolean,
                       overwriteFilters: Array[Filter] = Array.empty,
                       dynamicPartitionOverwrite: Boolean = false) extends BatchWrite {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)

  private var partitionInfo: (String, String) = _
  logger.info("HoloBatchWriter begin: " + LocalDateTime.now())
  private val columnNames: Array[String] = {
    val columnNames = new Array[String](sparkSchema.fields.length)
    for (i <- sparkSchema.fields.indices) {
      columnNames(i) = sparkSchema.fields.apply(i).name
    }
    columnNames
  }
  private var holoSchema : TableSchema = null

  if (is_overwrite && "stage" != hologresConfigs.writeMode) {
    hologresConfigs.tempTableForOverwrite = JDBCUtil.generateTempTableNameForOverwrite(hologresConfigs)
    JDBCUtil.createTempTableForOverWrite(hologresConfigs)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logger.info("HoloBatchWriter commit: " + LocalDateTime.now())
    if ("stage" == hologresConfigs.writeMode) {
      val commitStages = new Array[String](messages.length)
      for (i <- messages.indices) {
        messages(i) match {
          case message: StageWriterCommitMessage =>
            commitStages(i) = message.stageName
        }
      }
      if (hologresConfigs.copyStageOnly) {
        return
      }
      // 逻辑分区表 PARTITION 子句的分区值:
      // - 用户显式配置了 target_partition_values: 字面执行, 不与实际数据求交
      //   (overwrite 时声明但无数据的分区会被清空, 与 Hive INSERT OVERWRITE PARTITION 一致)
      // - 未配置: 从 SQL PARTITION 语法的 overwrite filters 解析, 否则从各 task 实际写入数据中采集
      val partitionColumnNames = hologresConfigs.writeTargetPartitionColumns
      var finalPartitionValues = Array.empty[Array[String]]
      if (partitionColumnNames.nonEmpty) {
        finalPartitionValues = if (hologresConfigs.writeTargetPartitionValues.nonEmpty) {
          hologresConfigs.writeTargetPartitionValues
        } else {
          val filterDeclaredValues =
            StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(overwriteFilters, partitionColumnNames)
          if (filterDeclaredValues.nonEmpty) {
            filterDeclaredValues
          } else {
            StagePartitionUtil.collectActualPartitionValues(messages)
          }
        }
        logger.info(s"Final partitions to ${if (is_overwrite) "overwrite" else "insert"}: " +
          finalPartitionValues.map(_.mkString("(", ",", ")")).mkString(","))
        if (finalPartitionValues.isEmpty) {
          logger.warn("No partition data collected from written data, skip insert from stages")
          JDBCUtil.dropStages(hologresConfigs, commitStages)
          return
        }
      }
      try {
        JDBCUtil.insertFromStages(hologresConfigs, holoSchema, columnNames.toList.asJava,
          commitStages.toList.asJava, hologresConfigs.holoConfig.getOnConflictAction, is_overwrite,
          partitionColumnNames,
          finalPartitionValues)
      } finally {
        JDBCUtil.dropStages(hologresConfigs, commitStages)
      }
    } else if (is_overwrite) {
      if (partitionInfo.eq(null)) {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs)
      } else {
        JDBCUtil.renameTempTableForOverWrite(hologresConfigs, partitionInfo._1, partitionInfo._2)
      }
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn("HoloBatchWriter abort: " + LocalDateTime.now())
    val stageNames = messages.filter(_ != null).collect {
      case m: StageWriterCommitMessage => m.stageName
    }
    if (stageNames.nonEmpty) {
      try {
        logger.info("HoloBatchWriter abort: drop stages " + stageNames.mkString(","))
        JDBCUtil.dropStages(hologresConfigs, stageNames)
      } catch {
        case e: Exception =>
          logger.warn("Failed to drop stages on batch abort", e)
      }
    }
    if (is_overwrite) {
      JDBCUtil.deleteTempTableForOverWrite(hologresConfigs)
    }
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): HoloWriterFactory = {
    val numPartitions = physicalWriteInfo.numPartitions()
    val holoClient: HoloClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      holoSchema = holoClient.getTableSchema(TableName.valueOf(hologresConfigs.table))
      if (holoSchema.isPartitionParentTable && is_overwrite) {
        throw new IOException("Partition parent table can not be insert overwrite now.")
      }
      if (holoSchema.isLogicalPartitionedTable && is_overwrite && "stage" != hologresConfigs.writeMode) {
        // 非 stage 模式的 overwrite 走临时表+rename 全表替换, 会清掉逻辑分区表所有分区的数据
        throw new IOException(
          "Overwrite on logical partition table is only supported in stage write mode " +
            "to avoid replacing the whole table, " +
            s"please set write.mode = stage, current write.mode = ${hologresConfigs.writeMode}")
      }
      if (dynamicPartitionOverwrite && "stage" != hologresConfigs.writeMode) {
        throw new IOException(
          "Dynamic partition overwrite is only supported in stage write mode, " +
            s"current write.mode = ${hologresConfigs.writeMode}")
      }
      partitionInfo = JDBCUtil.getChildTablePartitionInfo(hologresConfigs)

      // 逻辑分区表 stage 自动模式: 用户未配置 target_partition_columns/values 时,
      // 从表元数据解析分区列, 后续由 writer 从实际数据采集分区值
      if ("stage" == hologresConfigs.writeMode
        && hologresConfigs.writeTargetPartitionColumns.isEmpty
        && holoSchema.isLogicalPartitionedTable
        && holoSchema.getLogicalPartitionColumnNames != null
        && holoSchema.getLogicalPartitionColumnNames.nonEmpty) {
        val metaColumns = holoSchema.getLogicalPartitionColumnNames
        hologresConfigs.writeTargetPartitionColumns = metaColumns
        logger.info(s"Auto-resolved target partition columns from table metadata: ${metaColumns.mkString(",")}")
      }

      if (is_overwrite && "stage" != hologresConfigs.writeMode) {
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
  private def getTargetShardList(partitionId: Int): String =
    TargetShardUtil.getTargetShardList(partitionId, numPartitions, shardCount)

  override def createWriter(
                             partitionId: Int,
                             taskId: Long): DataWriter[InternalRow] = {
    hologresConfigs.writeMode match {
      case "stage" =>
        val timestamp = System.currentTimeMillis()
        val appId: String = Option(hologresConfigs.sparkAppId).filter(_.nonEmpty).getOrElse("unknown")

        val suffix = s"_spark_job_${appId}_task_${taskId}_${timestamp}"
        val maxStageNameLen = 127
        var tableName = holoSchema.getTableName.replaceAll("[^a-zA-Z0-9_-]", "_")
        if (tableName.length + suffix.length > maxStageNameLen) {
          val maxTableLen = math.max(1, maxStageNameLen - suffix.length)
          tableName = tableName.take(maxTableLen)
        }
        val stageName: String = (tableName + suffix).replaceAll("[^a-zA-Z0-9_-]", "_")
        new LoggerWrapper(getClass).info(s"[HoloWriterFactory] createWriter stage mode: sparkAppId=${hologresConfigs.sparkAppId}, stageName=$stageName")
        JDBCUtil.createStage(hologresConfigs, stageName)
        new HoloDataStageWriter(hologresConfigs, sparkSchema, holoSchema, stageName, taskId = taskId.toString)
      case "insert" =>
        new HoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
      case "stream" | "bulk_load" | "bulk_load_on_conflict" | CopyMode.STREAM | CopyMode.BULK_LOAD | CopyMode.BULK_LOAD_ON_CONFLICT =>
        if (hologresConfigs.reshuffleByHoloDistributionKey) {
          new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, getTargetShardList(partitionId), taskId.toString)
        } else {
          new HoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, taskId = taskId.toString)
        }
    }
  }
}
