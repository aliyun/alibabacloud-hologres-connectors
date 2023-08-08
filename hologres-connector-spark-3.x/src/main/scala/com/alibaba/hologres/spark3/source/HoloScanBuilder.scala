package com.alibaba.hologres.spark3.source

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


/** HoloScanBuilder. */
class HoloScanBuilder(sourceOptions: Map[String, String],
                      schema: StructType) extends ScanBuilder {
  override def build(): Scan = {
    new HoloBatchScan(sourceOptions, schema)
  }
}

class HoloBatchScan(sourceOptions: Map[String, String],
                    sparkSchema: StructType) extends Scan with Batch with PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)

  val hologresConfigs: HologresConfigs = new HologresConfigs(sourceOptions)
  @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
  val holoSchema: TableSchema = holoClient.getTableSchema(hologresConfigs.table)
  lazy val inputPartitions: Array[HoloInputPartition] = {
    val shardCount = getShardCount(holoClient, holoSchema)
    val numSplits = math.min(hologresConfigs.scan_parallelism, shardCount)
    logger.info("split reading hologres table {} to {} partition", hologresConfigs.table, numSplits)

    val size = shardCount / numSplits
    var remain = shardCount % numSplits

    val inputPartitions = new Array[HoloInputPartition](numSplits)
    var start = 0
    for (i <- 0 until numSplits) {
      var end = 0
      if (remain > 0) {
        end = start + size + 1
        remain -= 1
      }
      else end = start + size
      inputPartitions(i) = new HoloInputPartition(start, end)
      start = end
    }
    inputPartitions
  }

  def readSchema: StructType = sparkSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = inputPartitions.toArray

  override def createReaderFactory: PartitionReaderFactory = this

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new HoloPartitionReader(hologresConfigs, inputPartition.asInstanceOf[HoloInputPartition].shardIdRange, holoSchema, sparkSchema)
  }
}
