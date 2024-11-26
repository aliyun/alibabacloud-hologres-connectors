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

import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowTimeStampMicroAccessor
import com.alibaba.hologres.org.apache.arrow.vector.TimeStampMicroVector

import java.util.TimeZone

class SparkArrowTimeStampMicroAccessor(timeStampMicroVector: TimeStampMicroVector) extends BaseArrowTimeStampMicroAccessor(timeStampMicroVector) {
  private val TIMEZONE_OFFSET = TimeZone.getDefault.getRawOffset
  override def get(index: Int): AnyRef = {
    if (this.isNullAt(index)) {
      null
    } else {
      Long.box((this.getMicroSeconds(index) / 1000 - TIMEZONE_OFFSET) * 1000)
    }
  }
}
