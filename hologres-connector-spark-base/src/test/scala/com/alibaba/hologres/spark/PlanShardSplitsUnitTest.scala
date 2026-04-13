package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.source.HoloInputPartitionSplitByShard
import com.alibaba.hologres.spark.utils.PartitionSplitUtils
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

@RunWith(classOf[JUnitRunner])
class PlanShardSplitsUnitTest extends AnyFunSuite with Matchers {

  private def defaultConfig(): Map[String, String] = Map(
    "username" -> "user",
    "password" -> "password",
    "jdbcurl" -> "jdbc:postgresql://localhost:5432/db",
    "table" -> "test_table"
  )

  private def asShard(part: Any) =
    part.asInstanceOf[HoloInputPartitionSplitByShard]

  test("shard split: numSplits=1 => single partition covering all shards") {
    val shardCount = 10

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "1"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planShardSplits(conf, shardCount)
    parts.length shouldBe 1

    val p = asShard(parts.head)
    p.start shouldBe 0
    p.end shouldBe 10
  }

  test("shard split: numSplits >= shardCount => one shard per partition as possible") {
    val shardCount = 4

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "10" // 大于 shardCount
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planShardSplits(conf, shardCount)
    parts.length shouldBe 4

    val ranges = parts.map(asShard)

    ranges(0).start shouldBe 0
    ranges(0).end shouldBe 1

    ranges(1).start shouldBe 1
    ranges(1).end shouldBe 2

    ranges(2).start shouldBe 2
    ranges(2).end shouldBe 3

    ranges(3).start shouldBe 3
    ranges(3).end shouldBe 4
  }

  test("shard split: uneven shardCount => first partitions get +1") {
    val shardCount = 5

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "2" // 5 / 2 => [0,3), [3,5)
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planShardSplits(conf, shardCount)
    parts.length shouldBe 2

    val p0 = asShard(parts(0))
    p0.start shouldBe 0
    p0.end shouldBe 3

    val p1 = asShard(parts(1))
    p1.start shouldBe 3
    p1.end shouldBe 5
  }

  test("shard split: numSplits <= 0 should be treated as 1") {
    val shardCount = 4

    val options = defaultConfig() ++ Map(
      "read.split.num" -> "0"
    )
    val conf = new HologresConfigs(options)

    val parts = PartitionSplitUtils.planShardSplits(conf, shardCount)
    parts.length shouldBe 1

    val p = asShard(parts.head)
    p.start shouldBe 0
    p.end shouldBe 4
  }
}
