package com.alibaba.hologres.spark3.source

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.BaseHoloPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class HoloPartitionReader(hologresConfigs: HologresConfigs,
                          query_options: String,
                          holoSchema: TableSchema,
                          sparkSchema: StructType,
                          targetShards: Array[Int] = null
                         )
  extends BaseHoloPartitionReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards) with PartitionReader[InternalRow]
