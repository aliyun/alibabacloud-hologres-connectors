package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.exception.{HoloClientException, HoloClientWithDetailsException}
import com.alibaba.hologres.client.model.{Record, TableSchema}
import com.alibaba.hologres.client.{HoloClient, Put}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.utils.LoggerWrapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/** BaseHoloJdbcDataWriter. */
abstract class BaseHoloDataWriter(
                                   hologresConfigs: HologresConfigs,
                                   sparkSchema: StructType,
                                   holoSchema: TableSchema) extends Logging {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)

  logger.debug("create new holo client")
  private val client: HoloClient = new HoloClient(hologresConfigs.holoConfig)

  private val recordLength: Int = sparkSchema.fields.length
  private val columnIdToHoloId: Array[Int] = new Array[Int](recordLength)
  private val fieldWriters: Array[FieldWriter] = {
    val fieldWriters = new Array[FieldWriter](recordLength)
    for (i <- 0 until recordLength) {
      val holoColumnIndex = holoSchema.getColumnIndex(sparkSchema.fields.apply(i).name)
      columnIdToHoloId(i) = holoColumnIndex
      fieldWriters.update(i, FieldWriterUtils.createFieldWriter(sparkSchema.fields.apply(i), hologresConfigs.writeRemoveU0000))
    }
    fieldWriters
  }

  def commit(): Null = {
    logger.debug("Commit....")
    try {
      client.flush()
    }
    catch {
      case e: HoloClientWithDetailsException =>
        var i = 0
        while (i < e.size) {
          val failedRecord: Record = e.getFailRecord(i)
          val cause: HoloClientException = e.getException(i)
          i += 1
          logger.error(s"Upsert data $failedRecord failed, caused by $cause")
        }
        throw new SparkHoloException(e)
      case e: HoloClientException =>
        throw new SparkHoloException(e)
    }
    null
  }

  def write(row: InternalRow): Unit = {
    if (null == row) {
      return
    }
    try {
      val put: Put = new Put(holoSchema)
      convertRowToHologresRecord(put, row)
      client.put(put)
    } catch {
      case e: HoloClientWithDetailsException =>
        var i = 0
        while (i < e.size) {
          val failedRecord: Record = e.getFailRecord(i)
          val cause: HoloClientException = e.getException(i)
          i += 1
          logger.error(s"Upsert data $failedRecord failed, caused by $cause")
        }
        throw new SparkHoloException(e)
      case e: HoloClientException =>
        throw new SparkHoloException(e)
    }
  }

  def abort(): Unit = {
    logger.debug("Abort....")
    close()
  }

  private def convertRowToHologresRecord(put: Put, row: InternalRow): Unit = {
    try {
      for (i <- 0 until recordLength) {
        val columnHoloId = columnIdToHoloId(i)
        if (!row.isNullAt(i)) {
          put.setObject(columnHoloId, fieldWriters.apply(i).writeValue(row, i))
        } else {
          put.setObject(columnHoloId, null)
        }
      }
    } catch {
      case e: Exception =>
        // 打印convert失败的数据行
        logger.error(s"convert spark InternalRow to Hologres Record failed, InternalRow $row, record $put.getRecord")
        throw new SparkHoloException(e)
    }
  }

  protected def close(): Unit = {
    if (client != null) {
      try {
        client.flush()
      } catch {
        case e: HoloClientException =>
          throw new SparkHoloException(e)
      } finally {
        client.close()
      }
    }
    logger.debug("Close....")
  }
}
