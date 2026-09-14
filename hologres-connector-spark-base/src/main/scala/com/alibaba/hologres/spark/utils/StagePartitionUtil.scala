package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.spark.sink.copy.StageWriterCommitMessage
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.sources.{EqualNullSafe, EqualTo, Filter}

/**
 * 逻辑分区表stage写入时, 处理PARTITION子句的分区值:
 * 1. 从各task的commit message中聚合实际写入数据的分区值(用户未显式配置分区时的自动模式)
 * 2. 解析Spark SupportsOverwrite.overwrite(filters)传入的分区条件
 *    (来自SQL INSERT OVERWRITE ... PARTITION (col = value) 静态分区语法)
 *
 * 用户显式配置了write.target_partition_columns/values时字面执行, 不经过本工具类处理.
 */
object StagePartitionUtil {

  /** 聚合所有task commit message中采集到的实际分区值, 去重并保持首次出现的顺序 */
  def collectActualPartitionValues(messages: Array[WriterCommitMessage]): Array[Array[String]] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[Seq[String]]
    for (message <- messages) {
      message match {
        case stageMessage: StageWriterCommitMessage =>
          stageMessage.partitionValues.foreach(v => seen += v.toSeq)
        case _ =>
      }
    }
    seen.map(_.toArray).toArray
  }

  /**
   * 解析Spark overwrite(filters)传入的分区条件.
   * SQL `INSERT OVERWRITE t PARTITION (c1 = v1, c2 = v2)` 的静态分区spec会被Spark翻译为
   * 一组filter, 对应一个分区. 两种入口的filter类型不同:
   * - SQL 静态分区(INSERT OVERWRITE ... PARTITION) → EqualNullSafe
   * - DataFrame writeTo().overwrite(col === v) → EqualTo
   * 因此两者都需识别; 且分区列必须全部出现在filters中, 否则返回空数组
   * (退回到用户配置的values或writer自动采集).
   */
  def parsePartitionValuesFromOverwriteFilters(filters: Array[Filter],
                                               partitionColumnNames: Array[String]): Array[Array[String]] = {
    if (filters == null || filters.isEmpty || partitionColumnNames == null || partitionColumnNames.isEmpty) {
      return Array.empty[Array[String]]
    }
    val columnIndex = partitionColumnNames.zipWithIndex.toMap
    val row = new Array[String](partitionColumnNames.length)
    filters.foreach {
      case EqualTo(attribute, value) =>
        columnIndex.get(attribute).foreach(idx => row(idx) = String.valueOf(value))
      case EqualNullSafe(attribute, value) =>
        columnIndex.get(attribute).foreach(idx => row(idx) = String.valueOf(value))
      case _ =>
    }
    if (row.exists(_ == null)) {
      Array.empty[Array[String]]
    } else {
      Array(row)
    }
  }
}
