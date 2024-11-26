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

import com.alibaba.hologres.client.copy.CopyUtil
import com.alibaba.hologres.client.copy.out.CopyOutInputStream
import com.alibaba.hologres.client.copy.out.arrow.ArrowReader
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.exception.SparkHoloException
import com.alibaba.hologres.spark.source.copy.arrow.SparkArrowVectorAccessorUtil
import com.alibaba.hologres.org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util
import scala.collection.JavaConverters._

class BaseHoloCopyPartitionReader(hologresConfigs: HologresConfigs,
                                  query: String,
                                  holoSchema: TableSchema,
                                  sparkSchema: StructType) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val copyContext: CopyContext = new CopyContext
  copyContext.init(hologresConfigs)

  val copySql: String = CopyUtil.buildCopyOutSql(query, true)
  logger.info("the bulk read copy query: {}", copySql)
  logger.info("the sparkSchema: {}", sparkSchema)
  copyContext.schema = holoSchema
  val coins: CopyOutInputStream = new CopyOutInputStream(copyContext.manager.copyOut(copySql))
  copyContext.arrowReader = new ArrowReader(coins)

  var resultItor: Iterator[InternalRow] = _


  def next(): Boolean = {
    if (resultItor == null || !resultItor.hasNext) {
      if (copyContext.arrowReader.nextBatch()) {
        resultItor = convertArrowToInternalRows(copyContext.arrowReader.getCurrentValue)
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
    for (i <- 0 until rowCount) {
      val fieldsCount = root.getSchema.getFields.size()
      val res: Array[Any] = new Array[Any](fieldsCount)
      var j = 0
      for (field <- root.getSchema.getFields.asScala) {
        val vector: FieldVector = root.getVector(field.getName)
        val index = holoSchema.getColumnIndex(field.getName)
        if (index == null) {
          // 列裁剪导致sparkSchema为空(比如select count(*) 时), 返回的字段名在holoSchema中不存在
          if (sparkSchema.fields.length == 0) {
            res(j) = null
          } else {
            throw new SparkHoloException(s"column ${field.getName} not found in holo table ${holoSchema.getTableNameObj.getFullName}")
          }
        } else {
          val column = holoSchema.getColumn(index)
          val columnVectorAccessor = SparkArrowVectorAccessorUtil.createColumnVectorAccessor(vector, column)
          res(j) = columnVectorAccessor.get(i)
        }
        j += 1
      }
      result.add(new GenericInternalRow(res))
    }
    result.iterator().asScala
  }

  def close(): Unit = {
    if (copyContext.arrowReader != null) {
      try copyContext.arrowReader.close()
      catch {
        case e: IOException =>
          logger.warn("close fail", e)
          throw new IOException(e)
      } finally copyContext.arrowReader = null
    }
    copyContext.close()
    logger.debug("Close....")
  }

}
