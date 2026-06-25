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

import scala.collection.JavaConverters.seqAsJavaListConverter

case class StageWriterCommitMessage(stageName : String) extends WriterCommitMessage {}

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
    new SparkInternalRowArrowWriter(holoSchema, columnNames.toList.asJava, hologresConfigs.copyStageBatchSize, fieldWriters)
  private val stageWrapper: CopyInStageWrapper[InternalRow] =
    new CopyInStageWrapper(hologresConfigs.holoConfig, stageName, filePrefix, internalRowArrowWriter, hologresConfigs.copyStageFileSize)
  if(hologresConfigs.holoConfig.getWriteRps() > 0) {
    stageWrapper.setRateLimiter(new RateLimiter(hologresConfigs.holoConfig.getWriteRps()))
  }

  def commit(): StageWriterCommitMessage = {
    logger.debug("Commit....")
    stageWrapper.flush()
    val message : StageWriterCommitMessage = StageWriterCommitMessage(stageName)
    message
  }

  def write(row: InternalRow): Unit = {
    if (null == row) {
      return
    }
    stageWrapper.putRecord(row.copy())
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

  protected def close(): Unit = {
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
