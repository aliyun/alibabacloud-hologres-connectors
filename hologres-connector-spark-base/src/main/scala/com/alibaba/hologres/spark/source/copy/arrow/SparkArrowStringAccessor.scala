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

import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowVarCharAccessor
import com.alibaba.hologres.org.apache.arrow.vector.VarCharVector
import org.apache.spark.unsafe.types.UTF8String

class SparkArrowStringAccessor(varCharVector: VarCharVector, precision: Int = -1) extends BaseArrowVarCharAccessor(varCharVector, precision) {
  override def get(rowId: Int): AnyRef = {
    if (this.isNullAt(rowId)) {
      null
    } else {
      UTF8String.fromBytes(this.getBytes(rowId))
    }
  }
}
