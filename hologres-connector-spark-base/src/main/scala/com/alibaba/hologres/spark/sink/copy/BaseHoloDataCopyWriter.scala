package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.Put
import com.alibaba.hologres.client.copy.in.CopyInWrapper
import com.alibaba.hologres.client.copy.{CopyFormat, CopyMode}
import com.alibaba.hologres.client.exception.{HoloClientException, HoloClientWithDetailsException}
import com.alibaba.hologres.client.model.{Record, TableSchema}
import com.alibaba.hologres.client.utils.{RateLimiter, RecordChecker}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.sink._
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import java.io.IOException
import java.sql.{Connection, SQLException}
import scala.collection.JavaConverters.seqAsJavaListConverter

/** BaseHoloJdbcDataWriter. */
abstract class BaseHoloDataCopyWriter(
                                       hologresConfigs: HologresConfigs,
                                       sparkSchema: StructType,
                                       holoSchema: TableSchema,
                                       targetShardList: String = "",
                                       taskId: String = "") extends Logging {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setSparkTaskId(taskId)
  logger.setHoloTableName(hologresConfigs.table)

  private val copyMode: CopyMode = hologresConfigs.writeMode match {
    case mode: CopyMode => mode
    case _ => CopyMode.STREAM
  }
  private val copyFormat: CopyFormat = if (hologresConfigs.writeCopyFormat == "binary" && copyMode == CopyMode.STREAM) CopyFormat.BINARY else CopyFormat.CSV
  private val conn = initConnection(hologresConfigs, targetShardList)
  private val copyInWrapper: CopyInWrapper = new CopyInWrapper(
    conn,
    holoSchema,
    sparkSchema.fields.map(_.name).toList.asJava,
    copyFormat,
    hologresConfigs.writeMode match {
      case mode: CopyMode => mode
      case _ => CopyMode.STREAM
    },
    hologresConfigs.onConflictAction,
    hologresConfigs.writeCopyMaxBufferSize
  )
  if(hologresConfigs.holoConfig.getWriteRps() > 0) {
    copyInWrapper.setRateLimiter(new RateLimiter(hologresConfigs.holoConfig.getWriteRps()))
  }

  private val recordLength: Int = sparkSchema.fields.length
  private val columnIdToHoloId: Array[Int] = new Array[Int](recordLength)
  private val fieldTypeCasters: Array[Caster] = new Array[Caster](recordLength)
  private val fieldWriters: Array[FieldWriter] = {
    val fieldWriters = new Array[FieldWriter](recordLength)
    for (i <- 0 until recordLength) {
      val holoColumnIndex = holoSchema.getColumnIndex(sparkSchema.fields.apply(i).name)
      columnIdToHoloId(i) = holoColumnIndex
      fieldWriters.update(i, FieldWriterUtils.createFieldWriter(sparkSchema.fields.apply(i), hologresConfigs.writeRemoveU0000))
      if (!hologresConfigs.writeStrictDataTypeCheck) {
        fieldTypeCasters.update(i, FieldWriterUtils.createCaster(holoSchema.getColumn(holoColumnIndex)))
      }
    }
    fieldWriters
  }

  def commit(): Null = {
    logger.debug("Commit....")
    copyInWrapper.flush()
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
      if (hologresConfigs.writeCopyDirtyDataCheck) {
        try {
          RecordChecker.check(record)
        }
        catch {
          case e: HoloClientException =>
            throw new IOException(String.format("failed to copy because dirty data, the error record is %s.", record), e)
        }
      }

      copyInWrapper.putRecord(record)
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
          var value = fieldWriters.apply(i).writeValue(row, i)
          if (!hologresConfigs.writeStrictDataTypeCheck) {
            value = fieldTypeCasters.apply(i).castValue(value)
          }
          put.setObject(columnHoloId, value)
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
    if (copyInWrapper != null) {
      try copyInWrapper.close()
      catch {
        case e: IOException =>
          logger.warn("close copyInWrapper fail", e)
          throw new IOException(e)
      }
    }
    if (conn != null) {
      try conn.close()
      catch {
        case e: IOException =>
          logger.warn("close connection fail", e)
          throw new IOException(e)
      }
    }
    logger.debug("Close....")
  }

  def initConnection(configs: HologresConfigs, targetShards: String = ""): Connection = {
    try {
      val conn = JDBCUtil.createConnection(configs)

      // 不抛出异常: copy不需要返回影响行数所以默认关闭,但此guc仅部分版本支持,而且设置失败不影响程序运行
      JDBCUtil.executeSql(conn, "SET hg_experimental_enable_fixed_dispatcher_affected_rows = off", ignoreException = true)
      JDBCUtil.executeSql(conn, "SET hg_experimental_parallel_copy_scale = 1", ignoreException = true)
      JDBCUtil.executeSql(conn, s"set statement_timeout = '${configs.statementTimeout}s'")
      // server less computing
      if (configs.enableServerlessComputing) {
        if (configs.writeMode == CopyMode.STREAM) {
          // stream mode 不支持serverless
          if (conn != null) {
            try conn.close()
            catch {
              case _: SQLException => // ignore
            }
          }
          throw new RuntimeException("STREAM copyMode is not supported use serverless computing now.")
        }
        JDBCUtil.executeSql(conn, "set hg_computing_resource = 'serverless'")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_query_priority = ${configs.serverlessComputingQueryPriority}")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_required_cores = 5")
      }
      if (configs.reshuffleByHoloDistributionKey && targetShards != "") {
        JDBCUtil.executeSql(conn, s"set hg_experimental_target_shard_list = '$targetShards'")
      }
      if (configs.writeMode == CopyMode.BULK_LOAD_ON_CONFLICT) {
        JDBCUtil.executeSql(conn, "set hg_experimental_copy_enable_on_conflict = on;", ignoreException = true)
        JDBCUtil.executeSql(conn, "set hg_experimental_affect_row_multiple_times_keep_last = on;")
      }
      if (configs.disableRightJoinInCopy && configs.writeMode == CopyMode.BULK_LOAD_ON_CONFLICT) {
        JDBCUtil.executeSql(conn, "set hg_experimental_disable_right_join_in_copy = on;", ignoreException = true)
      }

      logger.info("Connection created and GUCs set successfully")
      conn
    } catch {
      case e: SQLException =>
        if (null != conn) {
          try {
            conn.close()
          } catch {
            case _: SQLException =>
          }
        }
        throw new RuntimeException(e)
    }
  }

}
