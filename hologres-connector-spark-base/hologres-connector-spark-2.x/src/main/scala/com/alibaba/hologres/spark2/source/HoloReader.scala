package com.alibaba.hologres.spark2.source

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util

class HoloReader(
                  sourceOptions: Map[String, String],
                  sparkSchema: StructType) extends DataSourceReader {
  private val logger = LoggerFactory.getLogger(getClass)

  val hologresConfigs: HologresConfigs = new HologresConfigs(sourceOptions)
  @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
  val holoSchema: TableSchema = holoClient.getTableSchema(hologresConfigs.table)
  lazy val inputPartitions: util.List[InputPartition[InternalRow]] = {
    val shardCount = getShardCount(holoClient, holoSchema)
    val numSplits = math.min(hologresConfigs.scan_parallelism, shardCount)
    logger.info("split reading hologres table {} to {} partition", hologresConfigs.table, numSplits)

    val size = shardCount / numSplits
    var remain = shardCount % numSplits

    val inputPartitions = new util.ArrayList[InputPartition[InternalRow]](numSplits)
    var start = 0
    for (i <- 0 until numSplits) {
      var end = 0
      if (remain > 0) {
        end = start + size + 1
        remain -= 1
      }
      else end = start + size
      inputPartitions.add(i, new HoloInputPartition(hologresConfigs, holoSchema, sparkSchema, start, end))
      start = end
    }
    inputPartitions
  }

  override def readSchema(): StructType = sparkSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = inputPartitions
}
