package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.common.HoloSplitStrategyTestTrait
import com.alibaba.hologres.spark.source.{HoloInputPartitionSplitByPartition, HoloInputPartitionSplitByRange, HoloInputPartitionSplitByShard}
import com.alibaba.hologres.spark.utils.{PartitionSplitUtils, SparkHoloUtil}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

/**
 * 集成测试，在hologres上创建测算表和视图, 测试planInputPartitions函数的所有分片策略
 */
@RunWith(classOf[JUnitRunner])
class PlanInputPartitionsIntegrationTest extends AnyFunSuite with Matchers with HoloSplitStrategyTestTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestUtils()
    setupTestEnvironment()
  }


  // 在所有测试之后清理环境
  override def afterAll(): Unit = {
    cleanupTestEnvironment()
  }

  test("planInputPartitions with RANGE strategy integration test") {
    testPlanInputPartitionsWithRangeStrategyIntegrationTest(RANGE_TABLE)
    testPlanInputPartitionsWithRangeStrategyIntegrationTest(RANGE_VIEW)
  }

  def testPlanInputPartitionsWithRangeStrategyIntegrationTest(table: String): Unit = {

    var conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "1",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "3"
    ))

    // 获取表模式
    val tuple2 = SparkHoloUtil.getHoloSchema(conf)
    val schema = tuple2._1
    if (table.equals(RANGE_TABLE)) {
      tuple2._2 shouldBe "TABLE"
    } else {
      tuple2._2 shouldBe "VIEW"
    }


    var parts = PartitionSplitUtils.planInputPartitions(conf, schema)
    parts.length shouldBe 5
    var i = 0
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByRange]
        val rangePart = part.asInstanceOf[HoloInputPartitionSplitByRange]
        if (i == 0) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=id:int4, range=(-∞, 1)"
        } else if (i == 1) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=id:int4, range=[1, 4)"
        } else if (i == 2) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=id:int4, range=[4, 7)"
        } else if (i == 3) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=id:int4, range=[7, 10)"
        } else {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=id:int4, range=[10, +∞)"
        }
        i += 1
      }
    )

    // 测试amount字段
    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "range",
      "read.split.column" -> "amount",
      "read.split.lower_bound" -> "100.00",
      "read.split.upper_bound" -> "500.00",
      "read.split.num" -> "3"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, schema)
    parts.length shouldBe 5
    i = 0
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByRange]
        val rangePart = part.asInstanceOf[HoloInputPartitionSplitByRange]
        if (i == 0) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=amount:numeric, range=(-∞, 100.00)"
        } else if (i == 1) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=amount:numeric, range=[100.00, 233.33333333333333333333)"
        } else if (i == 2) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=amount:numeric, range=[233.33333333333333333333, 366.66666666666666666666)"
        } else if (i == 3) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=amount:numeric, range=[366.66666666666666666666, 500.00)"
        } else {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=amount:numeric, range=[500.00, +∞)"
        }
        i += 1
      }
    )

    // 测试created_date字段
    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "range",
      "read.split.column" -> "created_date",
      "read.split.lower_bound" -> "2023-02-10",
      "read.split.upper_bound" -> "2023-03-08",
      "read.split.num" -> "3"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, schema)
    parts.length shouldBe 5
    i = 0
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByRange]
        val rangePart = part.asInstanceOf[HoloInputPartitionSplitByRange]
        if (i == 0) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_date:date, range=(-∞, 2023-02-10)"
        } else if (i == 1) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_date:date, range=[2023-02-10, 2023-02-18)"
        } else if (i == 2) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_date:date, range=[2023-02-18, 2023-02-27)"
        } else if (i == 3) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_date:date, range=[2023-02-27, 2023-03-08)"
        } else {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_date:date, range=[2023-03-08, +∞)"
        }
        i += 1
      }
    )

    // 测试created_timestamp字段
    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "range",
      "read.split.column" -> "created_timestamp",
      "read.split.lower_bound" -> "2023-02-10 00:00:00",
      "read.split.upper_bound" -> "2023-03-08 00:00:00",
      "read.split.num" -> "3"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, schema)
    parts.length shouldBe 5
    i = 0
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByRange]
        val rangePart = part.asInstanceOf[HoloInputPartitionSplitByRange]
        if (i == 0) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_timestamp:timestamptz, range=(-∞, 2023-02-10 00:00:00)"
        } else if (i == 1) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_timestamp:timestamptz, range=[2023-02-10 00:00:00, 2023-02-18 16:00:00)"
        } else if (i == 2) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_timestamp:timestamptz, range=[2023-02-18 16:00:00, 2023-02-27 08:00:00)"
        } else if (i == 3) {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_timestamp:timestamptz, range=[2023-02-27 08:00:00, 2023-03-08 00:00:00)"
        } else {
          rangePart.toString shouldBe "HoloInputPartitionSplitByRange(column=created_timestamp:timestamptz, range=[2023-03-08 00:00:00, +∞)"
        }
        i += 1
      }
    )


  }

  test("planInputPartitions with PARTITION strategy integration test") {
    testPlanInputPartitionsWithPartitionStrategyIntegrationTest(PARTITION_PARENT_TABLE)
    testPlanInputPartitionsWithPartitionStrategyIntegrationTest(PARTITION_PARENT_VIEW)
    testPlanInputPartitionsWithPartitionStrategyIntegrationTest(PARTITION_SAME_DATA_VIEW)
  }

  def testPlanInputPartitionsWithPartitionStrategyIntegrationTest(table: String): Unit = {
    var conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "partition",
      "read.split.column" -> "partition_col",
      "read.split.num" -> "3"
    ))
    var parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 3
    val holoPartitionValues: java.util.Set[String] = new java.util.HashSet[String]()
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByPartition]
        val partitionPart = part.asInstanceOf[HoloInputPartitionSplitByPartition]
        partitionPart.partitionColumn shouldBe "partition_col"
        partitionPart.partitionValues.length shouldBe 1
        holoPartitionValues.add(partitionPart.partitionValues(0))
      }
    )
    holoPartitionValues.size shouldBe 3

    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "partition",
      "read.split.column" -> "partition_col",
      "read.split.num" -> "2"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 2
    holoPartitionValues.clear()
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByPartition]
        val partitionPart = part.asInstanceOf[HoloInputPartitionSplitByPartition]
        partitionPart.partitionColumn shouldBe "partition_col"
        holoPartitionValues.add(partitionPart.partitionValues(0))
        if (partitionPart.partitionValues.length == 2) {
          holoPartitionValues.add(partitionPart.partitionValues(1))
        }
      }
    )
    holoPartitionValues.size shouldBe 3
  }

  test("planInputPartitions with PARTITION strategy integration test for different data view") {
    testPlanInputPartitionsWithPartitionStrategyForDifferentDataView(PARTITION_DIFFERENT_DATA_VIEW)
  }

  def testPlanInputPartitionsWithPartitionStrategyForDifferentDataView(table: String): Unit = {
    var conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "partition",
      "read.split.column" -> "partition_col",
      "read.split.num" -> "5" // 由于是不同数据的union，分区总数为5个（原表3个+另一表2个）
    ))
    var parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 5 // 总共5个分区
    val holoPartitionValues: java.util.Set[String] = new java.util.HashSet[String]()
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByPartition]
        val partitionPart = part.asInstanceOf[HoloInputPartitionSplitByPartition]
        partitionPart.partitionColumn shouldBe "partition_col"
        partitionPart.partitionValues.foreach(value => holoPartitionValues.add(value))
      }
    )
    holoPartitionValues.size shouldBe 5 // 应该有5个不同的分区值

    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "partition",
      "read.split.column" -> "partition_col",
      "read.split.num" -> "2"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 2 // 分成2个split
    holoPartitionValues.clear()
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByPartition]
        val partitionPart = part.asInstanceOf[HoloInputPartitionSplitByPartition]
        partitionPart.partitionColumn shouldBe "partition_col"
        partitionPart.partitionValues.foreach(value => holoPartitionValues.add(value))
      }
    )
    holoPartitionValues.size shouldBe 5 // 但总共还是5个不同的分区值
  }

  test("planInputPartitions with SHARD strategy integration test") {
    testPlanInputPartitionsWithShardStrategyIntegrationTest(SHARD_TABLE)
    testPlanInputPartitionsWithShardStrategyIntegrationTest(SHARD_VIEW)
    testPlanInputPartitionsWithShardStrategyIntegrationTest(SHARD_DIFFERENT_DATA_VIEW)
    testPlanInputPartitionsWithShardStrategyIntegrationTest(SHARD_SAME_DATA_VIEW)
  }

  def testPlanInputPartitionsWithShardStrategyIntegrationTest(table: String): Unit = {
    var conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "shard",
      "read.split.num" -> "2"
    ))
    var parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 2
    var i = 0
    parts.foreach(
      part => {
        part shouldBe a[HoloInputPartitionSplitByShard]
        val shardPart = part.asInstanceOf[HoloInputPartitionSplitByShard]
        if (i == 0) {
          shardPart.toString shouldBe "HoloInputPartitionSplitByShard(start=0, end=10)"
        } else {
          shardPart.toString shouldBe "HoloInputPartitionSplitByShard(start=10, end=20)"
        }
        i += 1
      }
    )

    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "shard",
      "read.split.num" -> "5"
    ))

    parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 5
    parts.foreach(part => part shouldBe a[HoloInputPartitionSplitByShard])

    conf = createHologresConfigs(table, Map(
      "read.split.strategy" -> "shard",
      "read.split.num" -> "80"
    ))
    parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 20
    parts.foreach(part => part shouldBe a[HoloInputPartitionSplitByShard])
  }

  test("planInputPartitions with SHARD strategy integration test for different shard count view") {
    try {
      testPlanInputPartitionsWithShardStrategyIntegrationTest(DIFFERENT_SHARD_COUNT_VIEW)
    } catch {
      case e: IllegalArgumentException => e.getMessage.contains("has different shard count")
    }
  }
}
