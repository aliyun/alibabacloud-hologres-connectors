/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.spark.source.copy

import com.alibaba.hologres.client.copy.CopyFormat
import com.alibaba.hologres.client.copy.out.CopyOutWrapper
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.org.apache.arrow.vector.VectorSchemaRoot
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.source.copy.arrow.SparkArrowVectorAccessorUtil
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.sql.{Connection, SQLException}
import java.util
import scala.collection.JavaConverters._

class BaseHoloCopyPartitionReader(hologresConfigs: HologresConfigs,
                                  query_options: String,
                                  holoSchema: TableSchema,
                                  sparkSchema: StructType,
                                  targetShards: Array[Int] = null) {
  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)

  private val conn = initConnection(hologresConfigs)
  private val isCompressed: Boolean = hologresConfigs.readMode == "bulk_read_compressed"
  private val readColumns = sparkSchema.fields.map(_.name).toList.asJava
  private val copyOutWrapper: CopyOutWrapper = new CopyOutWrapper(
    conn,
    holoSchema,
    readColumns,
    if (isCompressed) CopyFormat.ARROW_LZ4 else CopyFormat.ARROW,
    java.util.Collections.emptyList[Integer](),
    query_options,
    hologresConfigs.readCopyMaxBufferSize
  )

  var resultItor: Iterator[InternalRow] = _


  def next(): Boolean = {
    if (resultItor == null || !resultItor.hasNext) {
      if (copyOutWrapper.hasNextBatch) {
        resultItor = convertArrowToInternalRows(copyOutWrapper.getVectorSchemaRoot)
      } else {
        return false
      }
    }
    resultItor.hasNext
  }

  def get(): InternalRow = {
    resultItor.next()
  }

  private def convertArrowToInternalRows(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val result = new util.ArrayList[InternalRow]

    val rowCount = root.getRowCount
    val fieldsCount = root.getSchema.getFields.size()
    for (i <- 0 until rowCount) {
      val res: Array[Any] = new Array[Any](fieldsCount)
      for (j <- 0 until fieldsCount) {
        val vector = root.getFieldVectors.get(j)
        if (readColumns.isEmpty) {
          // 列裁剪导致sparkSchema为空(比如select count(*) 时), 直接返回null
          res(j) = null
        } else {
          val index = holoSchema.getColumnIndex(readColumns.get(j))
          if (index == null) {
            throw new SparkHoloException(s"column ${readColumns.get(j)} not found in holo table ${holoSchema.getTableNameObj.getFullName}")
          } else {
            val column = holoSchema.getColumn(index)
            val columnVectorAccessor = SparkArrowVectorAccessorUtil.createColumnVectorAccessor(vector, column)
            res(j) = columnVectorAccessor.get(i)
          }
        }
      }
      result.add(new GenericInternalRow(res))
    }
    result.iterator().asScala
  }

  def close(): Unit = {
    if (copyOutWrapper != null) {
      try copyOutWrapper.close()
      catch {
        case e: IOException =>
          logger.warn("close fail", e)
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

  def initConnection(configs: HologresConfigs): Connection = {
    try {
      val conn = JDBCUtil.createConnection(configs)

      JDBCUtil.executeSql(conn, s"set statement_timeout = '${configs.statementTimeout}s'")
      // server less computing
      if (configs.enableServerlessComputing) {
        JDBCUtil.executeSql(conn, "set hg_computing_resource = 'serverless'")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_query_priority = ${configs.serverlessComputingQueryPriority}")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_required_cores = 5")
      }
      // 仅读取指定的shard
      if (targetShards != null && targetShards.length > 0) {
        JDBCUtil.executeSql(conn, s"SET hg_experimental_target_shard_list = '${targetShards.mkString(",")}'")
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
