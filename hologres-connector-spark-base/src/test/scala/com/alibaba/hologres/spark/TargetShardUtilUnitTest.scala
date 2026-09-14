package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.utils.TargetShardUtil
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TargetShardUtilUnitTest extends AnyFunSuite with Matchers {

  test("one Spark partition per shard never emits shardCount") {
    (0 until 4).map(TargetShardUtil.getTargetShardList(_, 4, 4)) shouldBe
      Seq("0", "1", "2", "3")
  }

  test("shards are assigned round-robin when counts differ") {
    TargetShardUtil.getTargetShardList(0, 3, 8) shouldBe "0,3,6"
    TargetShardUtil.getTargetShardList(1, 3, 8) shouldBe "1,4,7"
    TargetShardUtil.getTargetShardList(2, 3, 8) shouldBe "2,5"
  }

  test("partition without a shard returns an empty list") {
    TargetShardUtil.getTargetShardList(4, 5, 4) shouldBe ""
  }

  test("invalid inputs are rejected") {
    assertThrows[IllegalArgumentException](TargetShardUtil.getTargetShardList(0, 0, 4))
    assertThrows[IllegalArgumentException](TargetShardUtil.getTargetShardList(-1, 4, 4))
    assertThrows[IllegalArgumentException](TargetShardUtil.getTargetShardList(0, 4, -1))
  }
}
