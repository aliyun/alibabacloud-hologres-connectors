package com.alibaba.hologres.spark3.source

import org.apache.spark.sql.connector.read.InputPartition


class HoloInputPartition(start: Int, end: Int) extends InputPartition {
  val shardIdRange: (Int, Int) = (start, end)

  override def preferredLocations(): Array[String] = {
    Array(shardIdRange._1, shardIdRange._2).map(_.toString)
  }
}
