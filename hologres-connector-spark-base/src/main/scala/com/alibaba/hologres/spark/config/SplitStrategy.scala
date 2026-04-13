package com.alibaba.hologres.spark.config

/**
 * 分片策略枚举
 */
object SplitStrategy extends Enumeration {
  type SplitStrategy = Value
  val RANGE, PARTITION, SHARD = Value

  def fromString(strategy: String): Option[SplitStrategy] = {
    strategy.toLowerCase match {
      case "range" => Some(RANGE)
      case "partition" => Some(PARTITION)
      case "shard" => Some(SHARD)
      case _ => None
    }
  }
}
