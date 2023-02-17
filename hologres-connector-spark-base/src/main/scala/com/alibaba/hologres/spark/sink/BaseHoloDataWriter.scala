package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.exception.{HoloClientException, HoloClientWithDetailsException}
import com.alibaba.hologres.client.model.{Record, TableSchema}
import com.alibaba.hologres.client.{HoloClient, Put}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.table.{Column, ColumnType, TableColumn}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}
import java.time.LocalDate

/** BaseHoloJdbcDataWriter. */
abstract class BaseHoloDataWriter(
                                   hologresConfigs: HologresConfigs,
                                   sparkSchema: Option[StructType],
                                   holoSchema: TableSchema) extends Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  logger.debug("create new holo client")
  private val client: HoloClient = new BaseSourceProvider().getOrCreateHoloClient(hologresConfigs)

  private val tableColumn: TableColumn = new TableColumn(sparkSchema.get, holoSchema)
  private val columns: Array[Column] = tableColumn.getColumns
  private val columnIdToHoloId: Array[Int] = tableColumn.getColumnIdToHoloId

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
    columns.indices.foreach(idx => {
      val columnHoloId = columnIdToHoloId(idx)
      if (!row.isNullAt(idx)) {
        columns(idx).getColumnType match {
          case ColumnType.INT => put.setObject(columnHoloId, row.getInt(idx))
          case ColumnType.BIGINT => put.setObject(columnHoloId, row.getLong(idx))
          case ColumnType.FLOAT => put.setObject(columnHoloId, row.getFloat(idx))
          case ColumnType.DOUBLE => put.setObject(columnHoloId, row.getDouble(idx))
          case ColumnType.BOOLEAN => put.setObject(columnHoloId, row.getBoolean(idx))
          case ColumnType.TIMESTAMP => put.setObject(columnHoloId, new Timestamp(row.getLong(idx) / 1000))
          case ColumnType.BYTEA => put.setObject(columnHoloId, row.getBinary(idx))
          case ColumnType.DATE => put.setObject(columnHoloId, Date.valueOf(LocalDate.ofEpochDay(row.getLong(idx))))
          case ColumnType.DECIMAL =>
            put.setObject(columnHoloId, row.getDecimal(idx, columns(idx).getPrecision, columns(idx).getScale).toJavaBigDecimal)
          case ColumnType.INTA => put.setObject(columnHoloId, row.getArray(idx).toIntArray())
          case ColumnType.BIGINTA => put.setObject(columnHoloId, row.getArray(idx).toLongArray())
          case ColumnType.FLOATA => put.setObject(columnHoloId, row.getArray(idx).toFloatArray())
          case ColumnType.DOUBLEA => put.setObject(columnHoloId, row.getArray(idx).toDoubleArray())
          case ColumnType.BOOLEANA => put.setObject(columnHoloId, row.getArray(idx).toBooleanArray())
          case ColumnType.TEXTA => {
            put.setObject(columnHoloId, row.getArray(idx).toObjectArray(StringType).map(e => {
              // 与InternalRow get array 表现一致，当数组元素有null值时，使用默认值空字符串""
              if (e == null) {
                ""
              } else {
                e.toString
              }
            }))
          }
          case ColumnType.TEXT => put.setObject(columnHoloId, row.getString(idx))
        }
      } else {
        put.setObject(columnHoloId, null)
      }
    })
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
