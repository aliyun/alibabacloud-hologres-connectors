package com.alibaba.hologres.spark3.source

import com.alibaba.hologres.client.Command.getShardCount
import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}
import com.alibaba.hologres.spark3.source.copy.HoloCopyPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType


/** HoloScanBuilder. */
class HoloScanBuilder(hologresConfigs: HologresConfigs,
                      sparkSchema: StructType,
                      mockHoloSchemaForQuery: TableSchema = null)
  extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownLimit
    with SupportsPushDownRequiredColumns {

  private val logger = new LoggerWrapper(getClass)
  logger.setSparkAppName(hologresConfigs.sparkAppName)
  logger.setSparkAppId(hologresConfigs.sparkAppId)
  logger.setHoloTableName(hologresConfigs.table)

  private var pushedPredicate = Array.empty[Predicate]
  private var pushedLimit = 0
  private var finalSchema = sparkSchema

  override def build(): Scan = {
    if (hologresConfigs.isTableSource) {
      new HoloTableBatchScan(hologresConfigs, finalSchema, pushedPredicate, pushedLimit)
    } else {
      new HoloQueryBatchScan(hologresConfigs, finalSchema, mockHoloSchemaForQuery)
    }
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    if (hologresConfigs.readPushDownPredicate) {
      val (pushed, unSupported) = predicates.partition(JdbcDialects.get("jdbc:postgresql").compileExpression(_).isDefined)
      logger.info(s"push down predicates: ${pushed.mkString(",")}")
      logger.info(s"unsupported predicates: ${unSupported.mkString(",")}")
      this.pushedPredicate = pushed
      unSupported
    } else {
      predicates
    }
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate

  override def pushLimit(limit: Int): Boolean = {
    if (hologresConfigs.readPushDownLimit) {
      pushedLimit = limit
      return true
    }
    false
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val requiredCols = requiredSchema.fields.map(PartitioningUtils.getColName(_, caseSensitive = false))
      .toSet
    val fields = sparkSchema.fields.filter { field =>
      val colName = PartitioningUtils.getColName(field, caseSensitive = false)
      requiredCols.contains(colName)
    }
    finalSchema = StructType(fields)
  }

}

class HoloTableBatchScan(hologresConfigs: HologresConfigs,
                         sparkSchema: StructType,
                         pushedPredicates: Array[Predicate],
                         pushedLimit: Int) extends Scan with Batch with PartitionReaderFactory {
  @transient private val logger = new LoggerWrapper(getClass)

  @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
  val holoSchema: TableSchema = holoClient.getTableSchema(hologresConfigs.table)
  private lazy val inputPartitions: Array[HoloInputPartition] = {
    var shardCount: Int = -1
    try {
      shardCount = getShardCount(holoClient, holoSchema)
    } finally {
      holoClient.close()
    }
    val numSplits = math.min(hologresConfigs.readMaxTaskCount, shardCount)
    logger.info(s"split reading hologres table ${hologresConfigs.table} to $numSplits partition")

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
    val queryTemplate: String = JDBCUtil.getSimpleSelectFromTable(holoSchema.getTableNameObj.getFullName, sparkSchema.fields.map(_.name))
    val shardIdRange = inputPartition.asInstanceOf[HoloInputPartition].shardIdRange
    var filters: String = pushedPredicates.flatMap(JdbcDialects.get("jdbc:postgresql").compileExpression(_)).map(p => s"($p)").mkString(" AND ")
    filters = if (filters.nonEmpty) s" AND $filters" else ""
    val limit: String = if (pushedLimit > 0) s" LIMIT $pushedLimit" else ""
    val query: String = s"$queryTemplate WHERE hg_shard_id >= ${shardIdRange._1} AND hg_shard_id < ${shardIdRange._2} $filters $limit"
    if (hologresConfigs.readMode == "select") {
      new HoloPartitionReader(hologresConfigs, query, holoSchema, sparkSchema)
    } else {
      new HoloCopyPartitionReader(hologresConfigs, query, holoSchema, sparkSchema)
    }
  }
}


class HoloQueryBatchScan(hologresConfigs: HologresConfigs,
                         sparkSchema: StructType,
                         mockHoloSchema: TableSchema) extends Scan with Batch with PartitionReaderFactory {
  @transient private val logger = new LoggerWrapper(getClass)

  override def readSchema(): StructType = sparkSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val inputPartitions = new Array[HoloInputPartition](1)
    inputPartitions(0) = new HoloInputPartition(-1, -1)
    logger.info("split reading hologres only one partition because it's a query source")
    inputPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val query: String = JDBCUtil.getSimpleSelectFromQuery(hologresConfigs.query, sparkSchema.fields.map(_.name))
    if (hologresConfigs.readMode == "bulk_read") {
      new HoloCopyPartitionReader(hologresConfigs, query, mockHoloSchema, sparkSchema)
    } else {
      new HoloPartitionReader(hologresConfigs, query, mockHoloSchema, sparkSchema)
    }
  }
}
