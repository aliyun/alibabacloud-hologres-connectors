package com.alibaba.hologres.spark.source

import org.apache.spark.sql.connector.read.InputPartition

/**
 * shard分片
 * 每个分片包含起始和结束的shard id, 左闭右开 [start, end)
 */
class HoloInputPartitionSplitByShard(val start: Int, val end: Int) extends InputPartition {
  override def toString: String =
    s"HoloInputPartitionSplitByShard(start=$start, end=$end)" // [start, end)
}

/**
 * 分区分片
 * 每个分片包含一个或者多个holo分区表的分区字段
 */
class HoloInputPartitionSplitByPartition(
                                          val partitionColumn: String,
                                          val partitionValues: Array[String]
                                        ) extends InputPartition {

  override def toString: String = {
    val valuesPreview =
      if (partitionValues.length <= 5) partitionValues.mkString("[", ",", "]")
      else partitionValues.take(5).mkString("[", ",", s", ...] (total=${partitionValues.length})")

    s"HoloInputPartitionSplitByPartition(column=$partitionColumn, values=$valuesPreview)"
  }
}

/**
 * 范围分片
 * 包含分片的字段名, 每个分片的起始和结束位置, 左闭右开 [lowerBound, upperBound)
 * 最后一段可能是 [lowerBound, upperBound]（upperInclusive = true）
 */
class HoloInputPartitionSplitByRange(
                                      val splitColumn: String,
                                      val columnType: String,
                                      val lowerBound: String,
                                      val upperBound: String
                                    ) extends InputPartition {

  override def toString: String = {
    val lowerBoundStr = if (lowerBound == null) "(-∞" else s"[$lowerBound"
    val upperBoundStr = if (upperBound == null) "+∞)" else s"$upperBound)"
    s"HoloInputPartitionSplitByRange(column=$splitColumn:$columnType, range=$lowerBoundStr, $upperBoundStr"
  }
}
