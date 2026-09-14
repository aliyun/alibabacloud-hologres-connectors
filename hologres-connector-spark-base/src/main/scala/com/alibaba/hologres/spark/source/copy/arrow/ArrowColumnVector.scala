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

package com.alibaba.hologres.spark.source.copy.arrow

import com.alibaba.hologres.client.copy.out.arrow.accessor._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

/**
 * 将holo copy out产生的Arrow FieldVector包装为Spark ColumnVector, 读取时无需再逐行物化为
 * GenericInternalRow, 由Spark的ColumnarToRow按需取值, 减少一次行存转换的开销.
 *
 * 各getter语义与行式路径的Spark*Accessor保持一致: date返回epoch day, timestamp返回微秒
 * (时区转换在accessor.get内完成), decimal与字符串取值同SparkArrowDecimalAccessor/
 * SparkArrowStringAccessor.
 */
class ArrowColumnVector(dataType: DataType, accessor: AbstractArrowVectorAccessor) extends ColumnVector(dataType) {

  override def close(): Unit = {
    accessor.close()
  }

  override def hasNull: Boolean = true

  override def numNulls(): Int = {
    throw new UnsupportedOperationException
  }

  override def isNullAt(rowId: Int): Boolean = {
    accessor.isNullAt(rowId)
  }

  override def getBoolean(rowId: Int): Boolean = {
    accessor.asInstanceOf[BaseArrowUInt1Accessor].getUInt1(rowId) != 0
  }

  override def getByte(rowId: Int): Byte = {
    accessor.asInstanceOf[BaseArrowUInt1Accessor].getUInt1(rowId)
  }

  override def getShort(rowId: Int): Short = {
    accessor.asInstanceOf[BaseArrowSmallIntAccessor].getShort(rowId)
  }

  override def getInt(rowId: Int): Int = {
    accessor match {
      case intAccessor: BaseArrowIntAccessor => intAccessor.getInt(rowId)
      // DateType: Spark取epoch day, 与SparkArrowDateDayAccessor行为一致
      case dateAccessor: BaseArrowDateDayAccessor => dateAccessor.getEpochDay(rowId)
      case other => other.get(rowId).asInstanceOf[Int]
    }
  }

  override def getLong(rowId: Int): Long = {
    accessor match {
      case bigIntAccessor: BaseArrowBigIntAccessor => bigIntAccessor.getLong(rowId)
      // TimestampType: SparkArrowTimeStampMicroAccessor/SparkArrowDateMilliAccessor
      // 的get已返回带时区转换的微秒值
      case other => other.get(rowId).asInstanceOf[Long]
    }
  }

  override def getFloat(rowId: Int): Float = {
    accessor.asInstanceOf[BaseArrowFloat4Accessor].getFloat(rowId)
  }

  override def getDouble(rowId: Int): Double = {
    accessor.asInstanceOf[BaseArrowFloat8Accessor].getDouble(rowId)
  }

  override def getArray(rowId: Int): ColumnarArray = {
    throw new UnsupportedOperationException
  }

  override def getMap(ordinal: Int): ColumnarMap = {
    throw new UnsupportedOperationException
  }

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    Decimal.apply(accessor.asInstanceOf[BaseArrowDecimalAccessor].getDecimal(rowId))
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    UTF8String.fromBytes(accessor.asInstanceOf[BaseArrowVarCharAccessor].getBytes(rowId))
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    accessor.asInstanceOf[BaseArrowVarBinaryAccessor].getBinary(rowId)
  }

  override def getChild(ordinal: Int): ColumnVector = {
    throw new UnsupportedOperationException
  }
}

object ArrowColumnVector {
  /**
   * 判断schema是否支持列式读取, 包含不支持的类型(如数组类型)时应回退到行式读取;
   * schema为空时(比如select count(*))也回退到行式读取.
   */
  def supportColumnarReads(sparkSchema: StructType): Boolean = {
    sparkSchema.fields.nonEmpty && sparkSchema.fields.forall(field => field.dataType match {
      case BooleanType | ShortType | IntegerType | LongType | FloatType | DoubleType |
           StringType | BinaryType | DateType | TimestampType => true
      case _: DecimalType => true
      case _ => false
    })
  }
}
