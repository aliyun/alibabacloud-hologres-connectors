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
import com.alibaba.hologres.client.model.Column
import com.alibaba.hologres.org.apache.arrow.vector._
import com.alibaba.hologres.org.apache.arrow.vector.complex.ListVector

import java.sql.Types


/**
 * AbstractArrowVectorAccessorUtil.
 */
object SparkArrowVectorAccessorUtil {
  def createColumnVectorAccessor(vector: FieldVector, column: Column): AbstractArrowVectorAccessor = column.getType match {
    case Types.INTEGER =>
      new BaseArrowIntAccessor(vector.asInstanceOf[IntVector])
    case Types.BIGINT =>
      new BaseArrowBigIntAccessor(vector.asInstanceOf[BigIntVector])
    case Types.SMALLINT =>
      new BaseArrowSmallIntAccessor(vector.asInstanceOf[SmallIntVector])
    case Types.FLOAT | Types.REAL =>
      new BaseArrowFloat4Accessor(vector.asInstanceOf[Float4Vector])
    case Types.DOUBLE =>
      new BaseArrowFloat8Accessor(vector.asInstanceOf[Float8Vector])
    case Types.DECIMAL | Types.NUMERIC =>
      new SparkArrowDecimalAccessor(vector.asInstanceOf[DecimalVector], column.getScale)
    case Types.BOOLEAN | Types.BIT =>
      new BaseArrowUInt1Accessor(vector.asInstanceOf[UInt1Vector])
    case Types.CHAR =>
      new SparkArrowStringAccessor(vector.asInstanceOf[VarCharVector], column.getPrecision)
    case Types.VARCHAR =>
      new SparkArrowStringAccessor(vector.asInstanceOf[VarCharVector])
    case Types.BINARY | Types.VARBINARY =>
      new BaseArrowVarBinaryAccessor(vector.asInstanceOf[VarBinaryVector])
    case Types.TIMESTAMP | Types.TIME_WITH_TIMEZONE =>
      if (column.getTypeName == "timestamptz") new SparkArrowDateMilliAccessor(vector.asInstanceOf[DateMilliVector])
      else new SparkArrowTimeStampMicroAccessor(vector.asInstanceOf[TimeStampMicroVector])
    case Types.TIME =>
      if (column.getTypeName == "timetz") new BaseArrowFixedSizeBinaryAccessor(vector.asInstanceOf[FixedSizeBinaryVector])
      else new BaseArrowTimeMicroAccessor(vector.asInstanceOf[TimeMicroVector])
    case Types.DATE =>
      new SparkArrowDateDayAccessor(vector.asInstanceOf[DateDayVector])
    case Types.ARRAY =>
      val elementAccessor: AbstractArrowVectorAccessor =
        createArrayElementVectorAccessor(vector.asInstanceOf[ListVector].getDataVector, column.getArrayElementType);
      new SparkArrowArrayAccessor(vector.asInstanceOf[ListVector], column.getArrayElementType, elementAccessor)
    case Types.OTHER =>
      if (column.getTypeName == "jsonb") {
        throw new IllegalArgumentException(s"copy out with arrow format unsupported column type ${column.getTypeName} now.")
      } else if (column.getTypeName == "roaringbitmap") {
        new BaseArrowVarBinaryAccessor(vector.asInstanceOf[VarBinaryVector])
      } else {
        new SparkArrowStringAccessor(vector.asInstanceOf[VarCharVector])
      }
    case _ =>
      throw new IllegalArgumentException("Unsupported column type: " + column.getTypeName)
  }

  private def createArrayElementVectorAccessor(vector: FieldVector, `type`: Int): AbstractArrowVectorAccessor = `type` match {
    case Types.INTEGER =>
      new BaseArrowIntAccessor(vector.asInstanceOf[IntVector])
    case Types.BIGINT =>
      new BaseArrowBigIntAccessor(vector.asInstanceOf[BigIntVector])
    case Types.SMALLINT =>
      new BaseArrowSmallIntAccessor(vector.asInstanceOf[SmallIntVector])
    case Types.FLOAT | Types.REAL =>
      new BaseArrowFloat4Accessor(vector.asInstanceOf[Float4Vector])
    case Types.DOUBLE =>
      new BaseArrowFloat8Accessor(vector.asInstanceOf[Float8Vector])
    case Types.BOOLEAN | Types.BIT =>
      new BaseArrowUInt1Accessor(vector.asInstanceOf[UInt1Vector])
    case Types.CHAR | Types.VARCHAR =>
      new BaseArrowVarCharAccessor(vector.asInstanceOf[VarCharVector])
    case _ =>
      throw new IllegalArgumentException("Unsupported array element type: " + `type`)
  }
}
