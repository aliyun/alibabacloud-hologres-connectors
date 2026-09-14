package com.alibaba.hologres.spark.source


import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.{HoloInputPartitionSplitByPartition, HoloInputPartitionSplitByRange, HoloInputPartitionSplitByShard}
import com.alibaba.hologres.spark.source.copy.arrow.ArrowColumnVector
import com.alibaba.hologres.spark.utils.{LoggerWrapper, PartitionSplitUtils}
import com.alibaba.hologres.spark.source.copy.{HoloCopyPartitionBatchReader, HoloCopyPartitionReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    val (query_options, targetShards) = buildQueryOptions(inputPartition)
    if (hologresConfigs.readMode == "select" || sparkSchema.fields.isEmpty) {
      // 列裁剪后 schema 为空时(如 select count(*))退回 select 模式: COPY OUT 的 SQL 由
      // holo-client 按列名列表拼装, 没法表达常量投影. 分片在此之前已定, 不损失并行度.
      // 注意此处运行在 executor 上, logger 是 @transient 的, 不能在这里打日志
      new HoloPartitionReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards)
    } else {
      new HoloCopyPartitionReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards)
    }
  }

  // 列式读取: 开关开启且bulk_read/bulk_read_compressed且schema中均为支持的类型时, 直接将Arrow batch
  // 包装为ColumnarBatch返回, 避免逐行物化InternalRow的开销
  override def supportColumnarReads(inputPartition: InputPartition): Boolean = {
    hologresConfigs.readEnableColumnarRead && hologresConfigs.readMode != "select" &&
      ArrowColumnVector.supportColumnarReads(sparkSchema)
  }

  override def createColumnarReader(inputPartition: InputPartition): PartitionReader[ColumnarBatch] = {
    val (query_options, targetShards) = buildQueryOptions(inputPartition)
    new HoloCopyPartitionBatchReader(hologresConfigs, query_options, holoSchema, sparkSchema, targetShards)
  }

  private def buildQueryOptions(inputPartition: InputPartition): (String, Array[Int]) = {
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
    (query_options, targetShards.toArray)
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
    if (hologresConfigs.readMode == "bulk_read" && sparkSchema.fields.nonEmpty) {
      new HoloCopyPartitionReader(hologresConfigs, "", mockHoloSchema, sparkSchema)
    } else {
      // schema 为空时同上, 退回 select 模式避免空投影
      new HoloPartitionReader(hologresConfigs, "", mockHoloSchema, sparkSchema)
    }
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    hologresConfigs.readEnableColumnarRead && hologresConfigs.readMode == "bulk_read" &&
      ArrowColumnVector.supportColumnarReads(sparkSchema)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new HoloCopyPartitionBatchReader(hologresConfigs, "", mockHoloSchema, sparkSchema)
  }
}
