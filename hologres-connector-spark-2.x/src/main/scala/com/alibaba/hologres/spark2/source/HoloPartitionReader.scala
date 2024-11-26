package com.alibaba.hologres.spark2.source

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.BaseHoloPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType

class HoloPartitionReader(hologresConfigs: HologresConfigs, query: String, holoSchema: TableSchema, sparkSchema: StructType)
  extends BaseHoloPartitionReader(hologresConfigs, query, holoSchema, sparkSchema) with InputPartitionReader[InternalRow]
