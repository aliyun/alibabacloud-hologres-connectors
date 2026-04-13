package com.alibaba.hologres.spark

import com.alibaba.hologres.client.model.{Column, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.HoloInputPartitionSplitByRange
import com.alibaba.hologres.spark.utils.PartitionSplitUtils
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

import java.sql.Types

@RunWith(classOf[JUnitRunner])
class PlanRangeSplitsUnitTest extends AnyFunSuite with Matchers {

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

  /** 检查分片区间连续、不重叠 */
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

  // --------------------------- 数值类型 ---------------------------

  test("SMALLINT: simple range, numSplits=1") {
    val schema = mockSchemaWithColumn("col", Types.SMALLINT, "smallint")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "col",
      "read.split.lower_bound" -> "1",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "1"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 3

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])

    ranges.head.columnType shouldBe "smallint"

    ranges.head.upperBound shouldBe "1"
    ranges.last.lowerBound shouldBe "10"

    assertContinuousRanges(ranges)
  }

  test("INTEGER: range 0 to 100, numSplits=4") {
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

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 6

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])

    ranges.head.columnType shouldBe "int4"

    ranges.head.upperBound shouldBe "0"
    ranges.last.lowerBound shouldBe "100"

    assertContinuousRanges(ranges)
  }

  test("DECIMAL: simple decimal range") {
    val schema = mockSchemaWithColumn("amount", Types.DECIMAL, "decimal")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "amount",
      "read.split.lower_bound" -> "1.5",
      "read.split.upper_bound" -> "7.5",
      "read.split.num" -> "3"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 5

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])
    ranges.head.columnType shouldBe "decimal"

    // 4 个边界
    ranges.head.upperBound shouldBe "1.5"
    ranges.last.lowerBound shouldBe "7.5"

    assertContinuousRanges(ranges)
  }

  test("DOUBLE: simple float8 range") {
    val schema = mockSchemaWithColumn("score", Types.DOUBLE, "float8")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "score",
      "read.split.lower_bound" -> "0.000000",
      "read.split.upper_bound" -> "1.000000",
      "read.split.num" -> "2"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 4

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])
    ranges.head.columnType shouldBe "float8"

    // 4 个边界
    ranges.head.upperBound shouldBe "0.000000"
    ranges.last.lowerBound shouldBe "1.000000"

    assertContinuousRanges(ranges)
  }

  test("numSplits fallback to read.max_task_count when read.split.num missing") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "0",
      "read.split.upper_bound" -> "100",
      // 不提供 read.split.num，使用 read.max_task_count
      "read.max_task_count" -> "3"
    )
    val conf = new HologresConfigs(options)

    conf.numSplits shouldBe 3

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 5

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])

    // 4 个边界
    ranges.head.upperBound shouldBe "0"
    ranges.last.lowerBound shouldBe "100"

    assertContinuousRanges(ranges)
  }

  // --------------------------- DATE / TIMESTAMP ---------------------------

  test("DATE: simple date range") {
    val schema = mockSchemaWithColumn("dt", Types.DATE, "date")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "dt",
      "read.split.lower_bound" -> "2021-01-01",
      "read.split.upper_bound" -> "2021-01-05", // 4 天
      "read.split.num" -> "2"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 4

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])
    ranges.head.columnType shouldBe "date"

    // 4 个边界
    ranges.head.upperBound shouldBe "2021-01-01"
    ranges.last.lowerBound shouldBe "2021-01-05"

    assertContinuousRanges(ranges)
  }

  test("TIMESTAMP: simple timestamp range") {
    val schema = mockSchemaWithColumn("ts", Types.TIMESTAMP, "timestamp")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "ts",
      "read.split.lower_bound" -> "2021-01-01 00:00:00",
      "read.split.upper_bound" -> "2021-01-01 12:00:00",
      "read.split.num" -> "3"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planRangeSplits(conf, schema)
    parts.length shouldBe 5

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])
    ranges.head.columnType shouldBe "timestamp"

    ranges.head.upperBound shouldBe "2021-01-01 00:00:00"
    ranges.last.lowerBound shouldBe "2021-01-01 12:00:00"

    assertContinuousRanges(ranges)
  }

  // --------------------------- 异常场景 ---------------------------

  test("unsupported type (e.g. VARCHAR) should throw IllegalArgumentException") {
    val schema = mockSchemaWithColumn("name", Types.VARCHAR, "varchar")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "name",
      "read.split.lower_bound" -> "a",
      "read.split.upper_bound" -> "z",
      "read.split.num" -> "2"
    )
    val conf = new HologresConfigs(options)

    val ex = intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(conf, schema)
    }
    ex.getMessage should include("Range split does not support data type: varchar")
  }

  test("split column not found should throw IllegalArgumentException") {
    val schema = mock(classOf[TableSchema])
    when(schema.getColumnIndex(any[String])).thenReturn(-1)

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "not_exist",
      "read.split.lower_bound" -> "0",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "2"
    )
    val conf = new HologresConfigs(options)

    val ex = intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(conf, schema)
    }
    ex.getMessage should include("Split column not_exist not found")
  }

  test("upper <= lower should throw IllegalArgumentException") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val options1 = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "10",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "2"
    )
    val conf1 = new HologresConfigs(options1)

    val ex1 = intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(conf1, schema)
    }
    ex1.getMessage should include("splitUpperBound(10) must be greater than splitLowerBound(10)")

    val options2 = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "20",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "2"
    )
    val conf2 = new HologresConfigs(options2)

    val ex2 = intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(conf2, schema)
    }
    ex2.getMessage should include("splitUpperBound(10) must be greater than splitLowerBound(20)")
  }

  test("missing splitColumn / lower / upper should throw IllegalArgumentException") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val missingCol = new HologresConfigs(
      defaultConfig() ++ Map(
        "table" -> "t",
        "read.split.strategy" -> "range",
        // "read.split.column" missing
        "read.split.lower_bound" -> "0",
        "read.split.upper_bound" -> "10",
        "read.split.num" -> "2"
      )
    )
    intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(missingCol, schema)
    }

    val missingLower = new HologresConfigs(
      defaultConfig() ++ Map(
        "table" -> "t",
        "read.split.strategy" -> "range",
        "read.split.column" -> "id",
        // "read.split.lower_bound" missing
        "read.split.upper_bound" -> "10",
        "read.split.num" -> "2"
      )
    )
    intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(missingLower, schema)
    }

    val missingUpper = new HologresConfigs(
      defaultConfig() ++ Map(
        "table" -> "t",
        "read.split.strategy" -> "range",
        "read.split.column" -> "id",
        "read.split.lower_bound" -> "0",
        // "read.split.upper_bound" missing
        "read.split.num" -> "2"
      )
    )
    intercept[IllegalArgumentException] {
      PartitionSplitUtils.planRangeSplits(missingUpper, schema)
    }
  }

  test("numSplits <= 0 (via read.split.num) should be treated as 1") {
    val schema = mockSchemaWithColumn("id", Types.INTEGER, "int4")

    val options = defaultConfig() ++ Map(
      "table" -> "t",
      "read.split.strategy" -> "range",
      "read.split.column" -> "id",
      "read.split.lower_bound" -> "0",
      "read.split.upper_bound" -> "10",
      "read.split.num" -> "0"
    )
    val conf = new HologresConfigs(options)

    conf.numSplits shouldBe 0 // 原始配置值
    val parts = PartitionSplitUtils.planRangeSplits(conf, schema) // 内部 math.max(1, numSplits)
    parts.length shouldBe 3

    val ranges = parts.map(_.asInstanceOf[HoloInputPartitionSplitByRange])

    ranges.head.upperBound shouldBe "0"
    ranges.last.lowerBound shouldBe "10"

    assertContinuousRanges(ranges)
  }
}
