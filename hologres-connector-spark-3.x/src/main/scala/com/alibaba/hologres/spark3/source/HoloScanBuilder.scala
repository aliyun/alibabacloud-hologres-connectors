package com.alibaba.hologres.spark3.source


import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.{HoloInputPartitionSplitByPartition, HoloInputPartitionSplitByRange, HoloInputPartitionSplitByShard}
import com.alibaba.hologres.spark.utils.{LoggerWrapper, PartitionSplitUtils}
import com.alibaba.hologres.spark3.source.copy.HoloCopyPartitionReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer


/** HoloScanBuilder. */
class HoloScanBuilder(hologresConfigs: HologresConfigs,
                      sparkSchema: StructType,
                      holoSchema: TableSchema)
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
    if (hologresConfigs.sourceType.equals("QUERY")) {
      new HoloQueryBatchScan(hologresConfigs, finalSchema, holoSchema)
    } else {
      new HoloTableBatchScan(hologresConfigs, finalSchema, holoSchema, pushedPredicate, pushedLimit)
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
                         holoSchema: TableSchema,
                         pushedPredicates: Array[Predicate],
                         pushedLimit: Int) extends Scan with Batch with PartitionReaderFactory {
  @transient private val logger = new LoggerWrapper(getClass)

  private lazy val inputPartitions: Array[InputPartition] =
    PartitionSplitUtils.planInputPartitions(hologresConfigs, holoSchema)

  def readSchema: StructType = sparkSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    logger.info(s"split reading hologres table ${hologresConfigs.table} to ${inputPartitions.length} partition")
    for (i <- inputPartitions.indices) {
      logger.info(s"partition $i: ${inputPartitions(i)}")
    }
    inputPartitions
  }

  override def createReaderFactory: PartitionReaderFactory = this

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    var filters: String = pushedPredicates.flatMap(JdbcDialects.get("jdbc:postgresql").compileExpression(_)).map(p => s"($p)").mkString(" AND ")
    filters = if (filters.nonEmpty) s" AND $filters" else ""
    val limit: String = if (pushedLimit > 0) s" LIMIT $pushedLimit" else ""
    var query_options = ""
    val targetShards: ArrayBuffer[Int] = ArrayBuffer[Int]()

    inputPartition match {
      case shardPartition: HoloInputPartitionSplitByShard =>
        query_options = s"where true $filters $limit"
        for (i <- shardPartition.start until shardPartition.end) {
          targetShards.append(i)
        }
      case rangePartition: HoloInputPartitionSplitByRange =>
        val conditions = scala.collection.mutable.ListBuffer[String]()
        if (rangePartition.lowerBound != null) {
          conditions += s"${rangePartition.splitColumn} >= '${rangePartition.lowerBound}'::${rangePartition.columnType}"
        }
        if (rangePartition.upperBound != null) {
          conditions += s"${rangePartition.splitColumn} < '${rangePartition.upperBound}'::${rangePartition.columnType}"
        }
        val whereClause = if (conditions.nonEmpty) {
          conditions.mkString(" AND ")
        } else {
          "1=1" // 全表扫描（兜底，理论上不会触发）
        }
        query_options = s"where $whereClause $filters $limit"
      case partitionPartition: HoloInputPartitionSplitByPartition =>
        val partitionValuesStr = partitionPartition.partitionValues.map(p => s"'$p'").mkString(",")
        query_options = s"where ${partitionPartition.partitionColumn} IN ($partitionValuesStr) $filters $limit"
    }
    if (hologresConfigs.readMode == "select") {
      new HoloPartitionReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards.toArray)
    } else {
      new HoloCopyPartitionReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards.toArray)
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
    val inputPartitions = new Array[HoloInputPartitionSplitByShard](1)
    inputPartitions(0) = new HoloInputPartitionSplitByShard(-1, -1)
    logger.info("split reading hologres only one partition because it's a query source")
    inputPartitions.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    if (hologresConfigs.readMode == "bulk_read") {
      new HoloCopyPartitionReader(hologresConfigs, "", mockHoloSchema, sparkSchema)
    } else {
      new HoloPartitionReader(hologresConfigs, "", mockHoloSchema, sparkSchema)
    }
  }
}
