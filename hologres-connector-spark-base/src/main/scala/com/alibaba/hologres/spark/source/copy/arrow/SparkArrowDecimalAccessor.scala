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

import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowDecimalAccessor
import com.alibaba.hologres.org.apache.arrow.vector.DecimalVector
import org.apache.spark.sql.types.Decimal


class SparkArrowDecimalAccessor(decimalVector: DecimalVector, scale: Int) extends BaseArrowDecimalAccessor(decimalVector, scale) {
  override def get(rowId: Int): AnyRef = {
    if (this.isNullAt(rowId)) {
      null
    } else {
      Decimal.apply(this.getDecimal(rowId))
    }
  }
}
