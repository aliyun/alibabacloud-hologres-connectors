package com.alibaba.hologres.spark2.source

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil
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
    if (hologresConfigs.isTableSource) {
      val queryTemplate: String = JDBCUtil.getSimpleSelectFromTable(holoSchema.getTableNameObj.getFullName, sparkSchema.fields.map(_.name))
      val query: String = queryTemplate + " WHERE hg_shard_id >= " + shardIdRange._1 + " and hg_shard_id < " + shardIdRange._2
      new HoloPartitionReader(hologresConfigs, query, holoSchema, sparkSchema)
    } else {
      val query: String = JDBCUtil.getSimpleSelectFromQuery(hologresConfigs.query, sparkSchema.fields.map(_.name))
      new HoloPartitionReader(hologresConfigs, query, holoSchema, sparkSchema)
    }
  }
}
