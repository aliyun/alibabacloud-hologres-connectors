package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.config.HologresConfigs
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for stage target partition config resolution in HologresConfigs.
 * The underlying quoted-CSV parser is covered in holo-client's CommonUtilTest.
 */
@RunWith(classOf[JUnitRunner])
class StagePartitionConfigUnitTest extends AnyFunSuite with Matchers {

  private def defaultConfig(): Map[String, String] = Map(
    "jdbcurl" -> "jdbc:postgresql://localhost:5432/testdb",
    "username" -> "testuser",
    "password" -> "testpass",
    "table" -> "test_table"
  )

  test("partition config: not configured -> empty arrays") {
    val conf = new HologresConfigs(defaultConfig())
    conf.writeTargetPartitionColumns shouldBe empty
    conf.writeTargetPartitionValues shouldBe empty
  }

  test("partition config: single column single partition") {
    val conf = new HologresConfigs(defaultConfig() ++ Map(
      "write.target_partition_columns" -> "\"ds\"",
      "write.target_partition_values" -> "\"20250602\""
    ))
    conf.writeTargetPartitionColumns shouldEqual Array("ds")
    conf.writeTargetPartitionValues.length shouldBe 1
    conf.writeTargetPartitionValues(0) shouldEqual Array("20250602")
  }

  test("partition config: single column multiple partitions") {
    val conf = new HologresConfigs(defaultConfig() ++ Map(
      "write.target_partition_columns" -> "\"ds\"",
      "write.target_partition_values" -> "\"20250101\"; \"20250102\"; \"20250103\""
    ))
    conf.writeTargetPartitionColumns shouldEqual Array("ds")
    conf.writeTargetPartitionValues.length shouldBe 3
    conf.writeTargetPartitionValues(0) shouldEqual Array("20250101")
    conf.writeTargetPartitionValues(1) shouldEqual Array("20250102")
    conf.writeTargetPartitionValues(2) shouldEqual Array("20250103")
  }

  test("partition config: multiple columns multiple partitions") {
    val conf = new HologresConfigs(defaultConfig() ++ Map(
      "write.target_partition_columns" -> "\"ds\", \"kind\"",
      "write.target_partition_values" -> "\"2025-01-11\", \"100\"; \"2025-01-12\", \"200\""
    ))
    conf.writeTargetPartitionColumns shouldEqual Array("ds", "kind")
    conf.writeTargetPartitionValues.length shouldBe 2
    conf.writeTargetPartitionValues(0) shouldEqual Array("2025-01-11", "100")
    conf.writeTargetPartitionValues(1) shouldEqual Array("2025-01-12", "200")
  }

  test("partition config: column count mismatch throws") {
    val ex = intercept[IllegalArgumentException] {
      new HologresConfigs(defaultConfig() ++ Map(
        "write.target_partition_columns" -> "\"ds\", \"kind\"",
        "write.target_partition_values" -> "\"2025-01-11\", \"100\"; \"2025-01-12\""
      ))
    }
    ex.getMessage should include("row #1")
    ex.getMessage should include("target_partition_columns has 2")
  }

  test("partition config: only columns without values throws") {
    val ex = intercept[IllegalArgumentException] {
      new HologresConfigs(defaultConfig() ++ Map(
        "write.target_partition_columns" -> "\"ds\""
      ))
    }
    ex.getMessage should include("must be set together")
  }

  test("partition config: only values without columns throws") {
    val ex = intercept[IllegalArgumentException] {
      new HologresConfigs(defaultConfig() ++ Map(
        "write.target_partition_values" -> "\"20250602\""
      ))
    }
    ex.getMessage should include("must be set together")
  }
}
