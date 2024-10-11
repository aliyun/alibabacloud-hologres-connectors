package com.alibaba.hologres.spark.source

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.utils.JDBCUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.sql.{PreparedStatement, ResultSet, SQLException}

class BaseHoloPartitionReader(hologresConfigs: HologresConfigs,
                              shardIdRange: (Int, Int),
                              holoSchema: TableSchema,
                              sparkSchema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)

  private var conn: PgConnection = _
  private var statement: PreparedStatement = _
  private var resultSet: ResultSet = _
  private val recordLength: Int = sparkSchema.fields.length
  private val fieldReaders: Array[FieldReader] = {
    val fieldReaders = new Array[FieldReader](recordLength)
    for (i <- 0 until recordLength) {
      val holoColumn = holoSchema.getColumn(holoSchema.getColumnIndex(sparkSchema.fields.apply(i).name))
      fieldReaders.update(i, FieldReaderUtils.createFieldReader(holoColumn.getType, holoColumn.getTypeName))
    }
    fieldReaders
  }
  init()

  def init(): Unit = {
    val queryTemplate: String = JDBCUtil.getSimpleSelectFromStatement(holoSchema.getTableNameObj.getFullName, sparkSchema.fields.map(_.name))
    val query: String = queryTemplate + " WHERE hg_shard_id >= " + shardIdRange._1 + " and hg_shard_id < " + shardIdRange._2
    logger.info("the bulk read query: {}", query)
    logger.info("the sparkSchema: {}", sparkSchema)

    conn = JDBCUtil.createConnection(hologresConfigs).unwrap(classOf[PgConnection])
    conn.setAutoCommit(false)
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"set statement_timeout = ${hologresConfigs.statementTimeout}")
      stmt.close()
    } catch {
      case e: SQLException =>
        logger.error(s"set statement_timeout to ${hologresConfigs.statementTimeout} failed.")
        throw new RuntimeException(e)
    }
    // server less computing
    if (hologresConfigs.enableServerlessComputing) {
      try {
        val stmt = conn.createStatement()
        stmt.execute("set hg_computing_resource = 'serverless'")
        stmt.close()
      } catch {
        case e: SQLException =>
          logger.error("set hg_computing_resource to serverless failed.")
          throw new RuntimeException(e)
      }
      try {
        val stmt = conn.createStatement()
        stmt.execute(s"SET hg_experimental_serverless_computing_query_priority = ${hologresConfigs.serverlessComputingQueryPriority}")
        stmt.close()
      } catch {
        case e: SQLException =>
          logger.error(s"set hg_experimental_serverless_computing_query_priority to ${hologresConfigs.serverlessComputingQueryPriority} failed.")
          throw new RuntimeException(e)
      }
    }

    statement = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(hologresConfigs.scan_batch_size)
    statement.setQueryTimeout(hologresConfigs.scan_timeout_seconds)
    resultSet = statement.executeQuery
  }

  def next(): Boolean = {
    resultSet.next
  }

  def get(): InternalRow = {
    convertHologresRecordToRow(resultSet)
  }

  private def convertHologresRecordToRow(resultSet: ResultSet): InternalRow = {
    val res: Array[Any] = new Array[Any](recordLength)
    for (i <- 0 until recordLength) {
      if (resultSet.getObject(i + 1) == null) {
        res(i) = null
      } else {
        res(i) = fieldReaders.apply(i).readValue(resultSet, i + 1)
      }
    }
    new GenericInternalRow(res)
  }

  def close(): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException =>
          throw new SparkHoloException(e)
      } finally {
        conn = null
      }
    }
    logger.debug("Close....")
  }

}
