package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.{HologresConfigs, SplitStrategy}
import com.alibaba.hologres.spark.source.{HoloInputPartitionSplitByPartition, HoloInputPartitionSplitByRange, HoloInputPartitionSplitByShard}
import org.apache.spark.sql.connector.read.InputPartition

import java.math.{RoundingMode, BigDecimal => JBigDecimal}
import java.sql.Types
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}


/**
 * 视图分片工具类
 */
object PartitionSplitUtils {

  private val logger = new LoggerWrapper(getClass)

  /**
   * 根据分片策略规划输入分区
   */
  def planInputPartitions(hologresConfigs: HologresConfigs, holoSchema: TableSchema): Array[InputPartition] = {
    var inputPartitions = Array.empty[InputPartition]
    val strategy = SplitStrategy.fromString(hologresConfigs.splitStrategy)
    if (strategy.isDefined) {
      strategy.get match {
        case SplitStrategy.RANGE =>
          inputPartitions = planRangeSplits(hologresConfigs, holoSchema)
        case SplitStrategy.PARTITION =>
          val partInfo = JDBCUtil.getPartitionColumnAndValues(hologresConfigs)
          inputPartitions = planPartitionSplits(hologresConfigs, partInfo._1, partInfo._2)
        case SplitStrategy.SHARD =>
          val shardCount = JDBCUtil.getShardCount(hologresConfigs)
          inputPartitions = planShardSplits(hologresConfigs, shardCount)
      }
    } else {
      // 如果没有指定分片策略，则返回单个分区
      inputPartitions = Array(new HoloInputPartitionSplitByShard(0, Int.MaxValue))
    }
    logger.info(s"split reading hologres table ${hologresConfigs.table} to ${inputPartitions.length} partition")
    for (i <- inputPartitions.indices) {
      logger.info(s"partition $i: ${inputPartitions(i)}")
    }
    inputPartitions
  }

  /**
   * 范围分片策略
   */
  def planRangeSplits(hologresConfigs: HologresConfigs, holoSchema: TableSchema): Array[InputPartition] = {
    if (hologresConfigs.splitColumn.isEmpty ||
      hologresConfigs.splitLowerBound.isEmpty ||
      hologresConfigs.splitUpperBound.isEmpty) {
      throw new IllegalArgumentException(
        "For range split strategy, read.split.column, read.split.lower_bound and read.split.upper_bound must be provided")
    }

    val numSplits = math.max(1, hologresConfigs.numSplits)
    val splitColumn = hologresConfigs.splitColumn
    val lowerBoundStr = hologresConfigs.splitLowerBound
    val upperBoundStr = hologresConfigs.splitUpperBound

    // 1. 获取类型
    val columnIndex = holoSchema.getColumnIndex(splitColumn)
    if (columnIndex == null || columnIndex < 0) {
      throw new IllegalArgumentException(s"Split column $splitColumn not found in holoSchema $holoSchema")
    }
    val column = holoSchema.getColumn(columnIndex)
    val sqlType = column.getType

    // 2. 定义：不同类型 -> BigDecimal 的映射
    val zoneId = ZoneId.systemDefault()
    val dateFmt = DateTimeFormatter.ISO_LOCAL_DATE
    val timestampFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def toDecimal(v: String): JBigDecimal = sqlType match {
      // 数字类型：直接用 BigDecimal 解析
      case Types.SMALLINT | Types.INTEGER | Types.BIGINT |
           Types.FLOAT | Types.REAL | Types.DOUBLE |
           Types.DECIMAL | Types.NUMERIC =>
        new JBigDecimal(v)

      // DATE：用 epochDay 作为“数字”
      case Types.DATE =>
        val d = LocalDate.parse(v, dateFmt)
        new JBigDecimal(d.toEpochDay)

      // TIMESTAMP：用 epochMillis 作为“数字”
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        val ldt = LocalDateTime.parse(v, timestampFmt)
        val instant = ldt.atZone(zoneId).toInstant
        new JBigDecimal(instant.toEpochMilli)

      case _ =>
        throw new IllegalArgumentException(
          s"Range split does not support data type: ${column.getTypeName}")
    }

    def fromDecimal(x: JBigDecimal): String = sqlType match {
      // 整数类型：转为不带小数的字符串
      case Types.SMALLINT | Types.INTEGER | Types.BIGINT =>
        x.setScale(0, RoundingMode.DOWN).toPlainString

      // 浮点类型：限制小数位，去掉多余 0
      case Types.FLOAT | Types.REAL | Types.DOUBLE =>
        x.setScale(6, RoundingMode.DOWN).toPlainString

      // DECIMAL/NUMERIC
      case Types.DECIMAL | Types.NUMERIC =>
        x.toPlainString

      // DATE：按 epochDay 还原 LocalDate
      case Types.DATE =>
        val epochDay = x.longValue()
        LocalDate.ofEpochDay(epochDay).format(dateFmt)

      // TIMESTAMP：按 epochMillis 还原 LocalDateTime
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        val epochMillis = x.longValue()
        LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zoneId).format(timestampFmt)

      case _ =>
        throw new IllegalStateException("Unexpected type in fromDecimal")
    }

    val lowerDec = toDecimal(lowerBoundStr)
    val upperDec = toDecimal(upperBoundStr)

    if (upperDec.compareTo(lowerDec) <= 0) {
      throw new IllegalArgumentException(
        s"splitUpperBound($upperBoundStr) must be greater than splitLowerBound($lowerBoundStr)")
    }

    // 3. 计算中间的 numSplits 个分片
    val range = upperDec.subtract(lowerDec)
    val step = range.divide(new JBigDecimal(numSplits), 20, RoundingMode.DOWN)

    val middleRanges = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    var start = lowerDec
    for (i <- 0 until numSplits) {
      val isLast = i == numSplits - 1
      val end = if (isLast) upperDec else start.add(step)
      middleRanges += (fromDecimal(start) -> fromDecimal(end))
      start = end
    }

    // 4. 构建最终分区列表：left overflow + middle + right overflow
    val allPartitions = scala.collection.mutable.ArrayBuffer.empty[InputPartition]

    // (1) Left overflow: (-∞, lowerBound)
    allPartitions += new HoloInputPartitionSplitByRange(
      splitColumn,
      column.getTypeName,
      lowerBound = null, // 表示 -∞
      upperBound = lowerBoundStr // 开区间上界
    )

    // (2) Middle ranges: [lower_i, upper_i)
    for (((lb, ub), i) <- middleRanges.zipWithIndex) {
      val isLast = i == middleRanges.size - 1
      allPartitions += new HoloInputPartitionSplitByRange(
        splitColumn,
        column.getTypeName,
        lb,
        ub
      )
    }

    // (3) Right overflow: (upperBound, +∞)
    allPartitions += new HoloInputPartitionSplitByRange(
      splitColumn,
      column.getTypeName,
      lowerBound = upperBoundStr, // 开区间下界
      upperBound = null // 表示 +∞
    )

    allPartitions.toArray
  }


  /**
   * 分区分片策略
   */
  def planPartitionSplits(hologresConfigs: HologresConfigs, holoPartitionColumn: String, holoPartitionValue: Array[String]): Array[InputPartition] = {
    // 要求所有的表都是分区表,且分区键相同

    val numSplits = math.min(math.max(1, hologresConfigs.numSplits), holoPartitionValue.length)
    logger.info(s"split reading hologres table ${hologresConfigs.table} to $numSplits partition")
    // 将holoPartitionValue分配为numSplits份
    val size = holoPartitionValue.length / numSplits
    var remain = holoPartitionValue.length % numSplits

    val inputPartitions = new Array[InputPartition](numSplits)
    var start = 0
    for (i <- 0 until numSplits) {
      var end = 0
      if (remain > 0) {
        end = start + size + 1
        remain -= 1
      } else {
        end = start + size
      }
      inputPartitions(i) = new HoloInputPartitionSplitByPartition(holoPartitionColumn, holoPartitionValue.slice(start, end))
      start = end
    }
    inputPartitions
  }

  /**
   * Shard分片策略
   */
  def planShardSplits(hologresConfigs: HologresConfigs, shardCount: Int): Array[InputPartition] = {
    // 要求所有的表都在同一个table_group, 且分布键一致
    val numSplits = math.min(math.max(1, hologresConfigs.numSplits), shardCount)
    logger.info(s"split reading hologres table ${hologresConfigs.table} to $numSplits partition")
    val size = shardCount / numSplits
    var remain = shardCount % numSplits

    val inputPartitions = new Array[InputPartition](numSplits)
    var start = 0
    for (i <- 0 until numSplits) {
      var end = 0
      if (remain > 0) {
        end = start + size + 1
        remain -= 1
      } else {
        end = start + size
      }
      inputPartitions(i) = new HoloInputPartitionSplitByShard(start, end)
      start = end
    }
    inputPartitions
  }

}

