package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.Put
import com.alibaba.hologres.client.copy.{CopyInOutputStream, CopyUtil, RecordBinaryOutputStream, RecordTextOutputStream}
import com.alibaba.hologres.client.exception.{HoloClientException, HoloClientWithDetailsException}
import com.alibaba.hologres.client.model.WriteMode.{INSERT_OR_IGNORE, INSERT_OR_UPDATE}
import com.alibaba.hologres.client.model.{Record, TableSchema}
import com.alibaba.hologres.client.utils.RecordChecker
import com.alibaba.hologres.org.postgresql.core.BaseConnection
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.sink._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.io.IOException

/** BaseHoloJdbcDataWriter. */
abstract class BaseHoloDataCopyWriter(
                                       hologresConfigs: HologresConfigs,
                                       sparkSchema: StructType,
                                       holoSchema: TableSchema) extends Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  private val copyContext: CopyContext = new CopyContext
  copyContext.init(hologresConfigs)
  private val binary: Boolean = hologresConfigs.copy_write_format == "binary"

  private val recordLength: Int = sparkSchema.fields.length
  private val columnIdToHoloId: Array[Int] = new Array[Int](recordLength)
  private val fieldWriters: Array[FieldWriter] = {
    val fieldWriters = new Array[FieldWriter](recordLength)
    for (i <- 0 until recordLength) {
      val holoColumnIndex = holoSchema.getColumnIndex(sparkSchema.fields.apply(i).name)
      columnIdToHoloId(i) = holoColumnIndex
      fieldWriters.update(i, FieldWriterUtils.createFieldWriter(holoSchema.getColumn(holoColumnIndex)))
    }
    fieldWriters
  }

  def commit(): Null = {
    logger.debug("Commit....")
    try {
      if (copyContext.os != null) copyContext.os.close()
    } finally copyContext.os = null
    null
  }

  def write(row: InternalRow): Unit = {
    if (null == row) {
      return
    }
    try {
      val put: Put = new Put(holoSchema)
      convertRowToHologresRecord(put, row)
      val record: Record = put.getRecord

      // record dirty data check
      if (hologresConfigs.copy_write_dirty_data_check) {
        try {
          RecordChecker.check(record)
        }
        catch {
          case e: HoloClientException =>
            throw new IOException(String.format("failed to copy because dirty data, the error record is %s.", record), e)
        }
      }

      // create copyContext in the first time
      if (copyContext.os == null) {
        val schema = record.getSchema
        copyContext.schema = schema
        val sql = CopyUtil.buildCopyInSql(record, binary, if (hologresConfigs.wMode eq INSERT_OR_IGNORE) INSERT_OR_IGNORE else INSERT_OR_UPDATE)
        logger.info("copy sql :{}", sql)
        val in = copyContext.manager.copyIn(sql)
        copyContext.os = if (binary) {
          new RecordBinaryOutputStream(new CopyInOutputStream(in), schema, copyContext.pgConn.unwrap(classOf[BaseConnection]), 1024 * 1024 * 10)
        } else {
          new RecordTextOutputStream(new CopyInOutputStream(in), schema, copyContext.pgConn.unwrap(classOf[BaseConnection]), 1024 * 1024 * 10)
        }
      }

      copyContext.os.putRecord(record)
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
    if (copyContext.os != null) {
      try copyContext.os.close()
      catch {
        case e: IOException =>
          logger.warn("close fail", e)
          throw new IOException(e)
      } finally copyContext.os = null
    }
    copyContext.close()
    logger.debug("Close....")
  }

}
