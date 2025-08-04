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

package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.RepartitionUtil
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/** HoloWriterBuilder. */
class HoloWriterBuilderV1(hologresConfigs: HologresConfigs,
                          schema: StructType) extends WriteBuilder with SupportsOverwrite {
  var is_overwrite: Boolean = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    is_overwrite = true
    this
  }

  override def build(): Write = {
    new HoloWriteV1(hologresConfigs, schema, is_overwrite)
  }
}

/** HoloWriteV1 [org.apache.spark.sql.connector.write.V1Write], it is required to use v1 write for reshuffle by holo distribution key. */
class HoloWriteV1(hologresConfigs: HologresConfigs,
                  sparkSchema: StructType,
                  is_overwrite: Boolean) extends V1Write {
  override def toInsertableRelation: InsertableRelation = {
    new HologresRelation(hologresConfigs, sparkSchema, is_overwrite)(SparkSession.active)
  }
}

class HologresRelation(hologresConfigs: HologresConfigs,
                       sparkSchema: StructType,
                       is_overwrite: Boolean)(@transient val sparkSession: SparkSession) extends BaseRelation with InsertableRelation {
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    RepartitionUtil.reShuffleThenWrite(data, hologresConfigs, saveMode = if (is_overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = sparkSchema
}
