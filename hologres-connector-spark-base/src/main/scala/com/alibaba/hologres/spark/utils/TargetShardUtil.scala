package com.alibaba.hologres.spark.utils

/** Utilities for assigning Hologres shards to Spark partitions. */
object TargetShardUtil {

  def getTargetShardList(partitionId: Int, numPartitions: Int, shardCount: Int): String = {
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(partitionId >= 0, s"partitionId must be non-negative, got $partitionId")
    require(shardCount >= 0, s"shardCount must be non-negative, got $shardCount")

    Iterator.iterate(partitionId)(_ + numPartitions)
      .takeWhile(_ < shardCount)
      .mkString(",")
  }
}
