package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.HoloInputPartitionSplitByPartition
import com.alibaba.hologres.spark.utils.PartitionSplitUtils
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

@RunWith(classOf[JUnitRunner])
class PlanPartitionSplitsUnitTest extends AnyFunSuite with Matchers {

  private def defaultConfig(): Map[String, String] = Map(
    "username" -> "user",
    "password" -> "password",
    "jdbcurl" -> "jdbc:postgresql://localhost:5432/db",
    "table" -> "test_table"
  )

  private def asPartition(part: Any) =
    part.asInstanceOf[HoloInputPartitionSplitByPartition]

  test("partition split: numSplits=1 => single partition with all values") {
    val holoPartitionColumn = "part"
    val holoPartitionValue = Array("20210101", "20210102")

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "1"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planPartitionSplits(conf, holoPartitionColumn, holoPartitionValue)
    parts.length shouldBe 1

    val p = asPartition(parts.head)
    p.partitionColumn shouldBe "part"
    p.partitionValues shouldEqual Array("20210101", "20210102")
  }

  test("partition split: numSplits >= partitions => each value in its own partition") {
    val holoPartitionColumn = "part"
    val holoPartitionValue = Array("20210101", "20210102") // 2 个值

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "10" // 大于分区数 2
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planPartitionSplits(conf, holoPartitionColumn, holoPartitionValue)
    parts.length shouldBe 2

    val p0 = asPartition(parts(0))
    p0.partitionColumn shouldBe "part"
    p0.partitionValues shouldEqual Array("20210101")

    val p1 = asPartition(parts(1))
    p1.partitionColumn shouldBe "part"
    p1.partitionValues shouldEqual Array("20210102")
  }

  test("partition split: uneven partition count => first partitions get +1") {
    val holoPartitionColumn = "p"
    val holoPartitionValue = Array("a", "b", "c", "d", "e") // 5 个

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "2" // 拆 2 份：3 + 2
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planPartitionSplits(conf, holoPartitionColumn, holoPartitionValue)
    parts.length shouldBe 2

    val p0 = asPartition(parts(0))
    p0.partitionColumn shouldBe "p"
    p0.partitionValues shouldEqual Array("a", "b", "c")

    val p1 = asPartition(parts(1))
    p1.partitionColumn shouldBe "p"
    p1.partitionValues shouldEqual Array("d", "e")
  }

  test("partition split: numSplits <= 0 should be treated as 1") {
    val holoPartitionColumn = "part"
    val holoPartitionValue = Array("20210101", "20210102")

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "0" // <= 0
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planPartitionSplits(conf, holoPartitionColumn, holoPartitionValue)
    parts.length shouldBe 1

    val p = asPartition(parts.head)
    p.partitionColumn shouldBe "part"
    p.partitionValues shouldEqual Array("20210101", "20210102")
  }
}
