package com.alibaba.hologres.spark

import com.alibaba.hologres.client.model.{Column, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.{HoloInputPartitionSplitByRange, HoloInputPartitionSplitByShard}
import com.alibaba.hologres.spark.utils.PartitionSplitUtils
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

import java.sql.Types

@RunWith(classOf[JUnitRunner])
class PlanInputPartitionsUnitTest extends AnyFunSuite with Matchers {

  private def mockSchemaWithColumn(colName: String, sqlType: Int, typeName: String): TableSchema = {
    val schema = mock(classOf[TableSchema])
    val col = mock(classOf[Column])

    when(schema.getColumnIndex(colName)).thenReturn(0)
    when(schema.getColumn(0)).thenReturn(col)
    when(col.getType).thenReturn(sqlType)
    when(col.getTypeName).thenReturn(typeName)

    schema
  }

  private def defaultConfig(): Map[String, String] = Map(
    "username" -> "user",
    "password" -> "password",
    "jdbcurl" -> "jdbc:postgresql://localhost:5432/db"
  )


  /** 检查分片区间连续、不重叠、最后一段是闭区间 */
  private def assertContinuousRanges(ranges: Seq[HoloInputPartitionSplitByRange]): Unit = {
    ranges should not be empty
    for (i <- 0 until ranges.length - 1) {
      val cur = ranges(i)
      val next = ranges(i + 1)
      // 上一段的 upper 就是下一段的 lower
      cur.upperBound shouldBe next.lowerBound
    }
    // 最后一段是闭区间
    ranges.head.lowerBound shouldBe null
    ranges.last.upperBound shouldBe null
  }

  // --------------------------- RANGE 分片策略 ---------------------------

  test("planInputPartitions with RANGE strategy") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "0",
      "read.split.upper_bound" -> "100",
      "read.split.num" -> "4"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planInputPartitions(conf, schema)
    parts.length shouldBe 6

    // 验证返回的是HoloInputPartitionSplitByRange类型的分区
    parts.foreach(part => part shouldBe a[HoloInputPartitionSplitByRange])

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])
    ranges.head.columnType shouldBe "int4"

    // 验证边界值
    ranges.head.upperBound shouldBe "0"
    ranges.last.lowerBound shouldBe "100"

    assertContinuousRanges(ranges)
  }

  // --------------------------- PARTITION 分片策略 ---------------------------

  test("planInputPartitions with PARTITION strategy") {
    // 对于PARTITION策略，我们需要mock JDBCUtil.getPartitionColumnAndValues方法
    // 但由于这是静态方法，我们需要使用PowerMock或其他工具来mock
    // 在这个简单的单元测试中，我们将跳过这部分测试，专注于测试其他部分
    cancel("Skipping PARTITION strategy test - requires PowerMock to mock static methods")
  }

  // --------------------------- SHARD 分片策略 ---------------------------

  test("planInputPartitions with SHARD strategy") {
    // 对于SHARD策略，我们需要mock JDBCUtil.getShardCount方法
    // 但由于这是静态方法，我们需要使用PowerMock或其他工具来mock
    // 在这个简单的单元测试中，我们将跳过这部分测试，专注于测试其他部分
    cancel("Skipping SHARD strategy test - requires PowerMock to mock static methods")
  }

  // --------------------------- 默认策略 ---------------------------

  test("planInputPartitions with no strategy specified (default to SHARD)") {
    val options = defaultConfig() ++ Map(
      "table" -> "test_table",
      // 不指定read.split.strategy，使用shard分片策略读取全部数据
      "read.split.strategy" -> ""
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 1

    // 验证返回的是HoloInputPartitionSplitByShard类型的分区
    parts.foreach(part => part shouldBe a[HoloInputPartitionSplitByShard])

    val shardPart = parts(0).asInstanceOf[HoloInputPartitionSplitByShard]
    shardPart.start shouldBe 0
    shardPart.end shouldBe Int.MaxValue
  }

  // --------------------------- 错误情况 ---------------------------

  test("planInputPartitions with unsupported strategy") {
    val options = defaultConfig() ++ Map(
      "table" -> "test_table",
      "read.split.strategy" -> "unsupported"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planInputPartitions(conf, null)
    parts.length shouldBe 1

    // 验证返回的是HoloInputPartitionSplitByShard类型的分区（默认行为）
    parts.foreach(part => part shouldBe a[HoloInputPartitionSplitByShard])
  }

  // --------------------------- RANGE 策略异常情况 ---------------------------

  test("planInputPartitions with RANGE strategy but missing parameters should throw exception") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id"
      // 缺少read.split.lower_bound和read.split.upper_bound
    )
    val conf = new HologresConfigs(options)

    val ex = intercept[IllegalArgumentException] {
      PartitionSplitUtils.planInputPartitions(conf, schema)
    }
    ex.getMessage should include("For range split strategy, read.split.column, read.split.lower_bound and read.split.upper_bound must be provided")
  }
}