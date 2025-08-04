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

import com.alibaba.hologres.client.copy.out.arrow.accessor.{AbstractArrowVectorAccessor, BaseArrowArrayAccessor}
import com.alibaba.hologres.org.apache.arrow.vector.complex.ListVector
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Types

class SparkArrowArrayAccessor(vector: ListVector, elementTyp: Int, elementAccessor: AbstractArrowVectorAccessor)
  extends BaseArrowArrayAccessor(vector, elementTyp, elementAccessor) {

  override def get(rowId: Int): AnyRef = {
    if (isNullAt(rowId)) return null
    val list = getArray(rowId)
    this.elementTyp match {
      case Types.VARCHAR =>
        ArrayData.toArrayData(list.toArray.map(_.asInstanceOf[String]).map(UTF8String.fromString))
      case Types.INTEGER | Types.BIGINT | Types.REAL | Types.DOUBLE | Types.BIT =>
        ArrayData.toArrayData(list.toArray)
      case _ =>
        getArray(rowId)
    }
  }

}
