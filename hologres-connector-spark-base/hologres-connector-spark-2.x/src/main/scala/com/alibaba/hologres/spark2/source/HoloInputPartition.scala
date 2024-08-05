package com.alibaba.hologres.spark2.source

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType


class HoloInputPartition(hologresConfigs: HologresConfigs, holoSchema: TableSchema, sparkSchema: StructType, start: Int, end: Int)
  extends InputPartition[InternalRow] {
  val shardIdRange: (Int, Int) = (start, end)

  override def preferredLocations(): Array[String] = {
    Array(shardIdRange._1, shardIdRange._2).map(_.toString)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new HoloPartitionReader(hologresConfigs, shardIdRange, holoSchema, sparkSchema)
  }
}
