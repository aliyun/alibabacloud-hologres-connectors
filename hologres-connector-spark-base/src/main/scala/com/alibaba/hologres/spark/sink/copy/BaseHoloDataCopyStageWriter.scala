package com.alibaba.hologres.spark.sink.copy


import com.alibaba.hologres.client.copy.in.CopyInStageWrapper
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.client.utils.RateLimiter
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink._
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable

/**
 * stage写入的commit message. partitionValues为该task实际写入数据中出现的分区值组合,
 * 用于在driver端自动确定 PARTITION 子句, 实现"有数据才写入/覆盖"的分区级语义
 */
case class StageWriterCommitMessage(stageName : String,
                                    partitionValues: Array[Array[String]] = Array.empty) extends WriterCommitMessage {}

abstract class BaseHoloDataCopyStageWriter(hologresConfigs: HologresConfigs,
                                           sparkSchema: StructType,
                                           holoSchema: TableSchema,
                                           stageName: String,
                                           taskId: String = "") extends Logging {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setSparkTaskId(taskId)
  logger.setHoloTableName(hologresConfigs.table)

  private val recordLength: Int = sparkSchema.fields.length
  private val columnNames: Array[String] = new Array[String](recordLength)
  private val columnIdToHoloId: Array[Int] = new Array[Int](recordLength)
  private val fieldTypeCasters: Array[Caster] = new Array[Caster](recordLength)
  private val fieldWriters: Array[FieldWriter] = {
    val fieldWriters = new Array[FieldWriter](recordLength)
    for (i <- 0 until recordLength) {
      val holoColumnIndex = holoSchema.getColumnIndex(sparkSchema.fields.apply(i).name)
      columnNames(i) = sparkSchema.fields.apply(i).name
      columnIdToHoloId(i) = holoColumnIndex
      fieldWriters.update(i, FieldWriterUtils.createFieldWriter(sparkSchema.fields.apply(i), hologresConfigs.writeRemoveU0000))
      if (!hologresConfigs.writeStrictDataTypeCheck) {
        fieldTypeCasters.update(i, FieldWriterUtils.createCaster(holoSchema.getColumn(holoColumnIndex)))
      }
    }
    fieldWriters
  }

  private val filePrefix : String = "file"
  private val internalRowArrowWriter: SparkInternalRowArrowWriter =
    new SparkInternalRowArrowWriter(holoSchema, columnNames.toList.asJava, hologresConfigs.copyStageBatchSize,
      fieldWriters, hologresConfigs.copyStageCompression)
  private val stageWrapper: CopyInStageWrapper[InternalRow] =
    new CopyInStageWrapper(hologresConfigs.holoConfig, stageName, filePrefix, internalRowArrowWriter, hologresConfigs.copyStageFileSize)
  if(hologresConfigs.holoConfig.getWriteRps() > 0) {
    stageWrapper.setRateLimiter(new RateLimiter(hologresConfigs.holoConfig.getWriteRps()))
  }

  // 配置了target_partition_columns时, 解析分区列在spark schema中的下标, 写入时自动采集实际分区值
  private val partitionColumnIndexes: Array[Int] = {
    if (hologresConfigs.writeTargetPartitionColumns.isEmpty) {
      Array.empty[Int]
    } else {
      hologresConfigs.writeTargetPartitionColumns.map { col =>
        val idx = sparkSchema.fields.indexWhere(_.name == col)
        if (idx < 0) {
          throw new IllegalArgumentException(
            s"write.target_partition_columns contains column '$col' which is missing in the input DataFrame's schema: $sparkSchema")
        }
        idx
      }
    }
  }
  private val collectedPartitionValues: mutable.HashSet[Seq[String]] = mutable.HashSet.empty

  def commit(): StageWriterCommitMessage = {
    logger.debug("Commit....")
    stageWrapper.flush()
    val message : StageWriterCommitMessage = StageWriterCommitMessage(stageName,
      collectedPartitionValues.map(_.toArray).toArray)
    message
  }

  def write(row: InternalRow): Unit = {
    if (null == row) {
      return
    }
    collectPartitionValues(row)
    stageWrapper.putRecord(row.copy())
  }

  private def collectPartitionValues(row: InternalRow): Unit = {
    if (partitionColumnIndexes.isEmpty) {
      return
    }
    val values = new Array[String](partitionColumnIndexes.length)
    var i = 0
    while (i < partitionColumnIndexes.length) {
      val idx = partitionColumnIndexes(i)
      if (row.isNullAt(idx)) {
        throw new IllegalArgumentException(
          s"Partition column '${hologresConfigs.writeTargetPartitionColumns(i)}' can not be null when writing logical partition table with target_partition_columns set")
      }
      values(i) = formatPartitionValue(row, idx)
      i += 1
    }
    collectedPartitionValues += values.toSeq
  }

  private def formatPartitionValue(row: InternalRow, idx: Int): String = {
    sparkSchema.fields(idx).dataType match {
      case DataTypes.DateType =>
        Date.valueOf(LocalDate.ofEpochDay(row.getInt(idx))).toString
      case DataTypes.TimestampType =>
        // 保留微秒精度, 并正确处理 1970 年之前的负微秒值(向零截断会导致时间偏移)
        val micros = row.getLong(idx)
        val ts = new Timestamp(Math.floorDiv(micros, 1000L))
        ts.setNanos(ts.getNanos + (Math.floorMod(micros, 1000L) * 1000L).toInt)
        ts.toString
      case dataType =>
        row.get(idx, dataType).toString
    }
  }

  def abort(): Unit = {
    logger.info("Abort, dropping stage: " + stageName)
    try {
      if (stageWrapper != null) {
        stageWrapper.abort()
      }
    } catch {
      case e: Exception =>
        logger.warn("Error aborting stageWrapper", e)
    }
    try {
      JDBCUtil.dropStages(hologresConfigs, Array(stageName))
    } catch {
      case e: Exception =>
        logger.warn("Failed to drop stage on abort: " + stageName, e)
    }
  }

  // DataWriter (java.io.Closeable) 要求 close() 为 public, Scala 2.13 会对 protected 覆写报
  // "weaker access privileges in overriding", 因此声明为 public; 2.12 同样接受
  def close(): Unit = {
    if (stageWrapper != null) {
      try {
        stageWrapper.close()
      } catch {
        case e: Exception =>
          logger.warn("Error closing stageWrapper", e)
      }
    }
    logger.debug("Close....")
  }

}
