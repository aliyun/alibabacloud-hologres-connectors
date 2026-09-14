package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.sink.copy.StageWriterCommitMessage
import com.alibaba.hologres.spark.utils.StagePartitionUtil
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.sources.{EqualNullSafe, EqualTo, Filter, GreaterThan}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.matchers.should.Matchers

/** Unit tests for StagePartitionUtil: aggregate actual partition values and parse overwrite filters. */
@RunWith(classOf[JUnitRunner])
class StagePartitionUtilUnitTest extends AnyFunSuite with Matchers {

  private def message(stageName: String, values: Array[Array[String]]): WriterCommitMessage = {
    StageWriterCommitMessage(stageName, values)
  }

  test("collect: aggregate and dedup partition values across tasks") {
    val messages: Array[WriterCommitMessage] = Array(
      message("s1", Array(Array("20250101"), Array("20250102"))),
      message("s2", Array(Array("20250102"), Array("20250103"))),
      message("s3", Array.empty)
    )
    val actual = StagePartitionUtil.collectActualPartitionValues(messages)
    actual.length shouldBe 3
    actual.map(_.toSeq) should contain theSameElementsAs Seq(Seq("20250101"), Seq("20250102"), Seq("20250103"))
  }

  test("collect: multi-column partition values") {
    val messages: Array[WriterCommitMessage] = Array(
      message("s1", Array(Array("20250101", "100"))),
      message("s2", Array(Array("20250101", "100"), Array("20250102", "200")))
    )
    val actual = StagePartitionUtil.collectActualPartitionValues(messages)
    actual.length shouldBe 2
    actual.map(_.toSeq) should contain theSameElementsAs Seq(Seq("20250101", "100"), Seq("20250102", "200"))
  }

  test("filters: EqualNullSafe filter (from SQL static PARTITION) -> one partition row") {
    val filters: Array[Filter] = Array(EqualNullSafe("ds", "20250101"))
    val parsed = StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds"))
    parsed.length shouldBe 1
    parsed(0) shouldEqual Array("20250101")
  }

  test("filters: mixed EqualTo and EqualNullSafe filters") {
    val filters: Array[Filter] = Array(EqualNullSafe("kind", "100"), EqualTo("ds", "20250101"))
    val parsed = StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds", "kind"))
    parsed.length shouldBe 1
    parsed(0) shouldEqual Array("20250101", "100")
  }

  test("filters: single EqualTo filter -> one partition row") {
    val filters: Array[Filter] = Array(EqualTo("ds", "20250101"))
    val parsed = StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds"))
    parsed.length shouldBe 1
    parsed(0) shouldEqual Array("20250101")
  }

  test("filters: multi-column EqualTo filters ordered by partition columns") {
    val filters: Array[Filter] = Array(EqualTo("kind", "100"), EqualTo("ds", "20250101"))
    val parsed = StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds", "kind"))
    parsed.length shouldBe 1
    parsed(0) shouldEqual Array("20250101", "100")
  }

  test("filters: non-EqualTo filter ignored and partial columns -> empty") {
    val filters: Array[Filter] = Array(EqualTo("ds", "20250101"), GreaterThan("id", 10))
    StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds", "kind")) shouldBe empty
  }

  test("filters: empty filters or empty columns -> empty") {
    StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(Array.empty, Array("ds")) shouldBe empty
    StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(null, Array("ds")) shouldBe empty
    StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(Array(EqualTo("ds", "20250101")), Array.empty) shouldBe empty
  }

  test("filters: attribute not in partition columns -> empty") {
    val filters: Array[Filter] = Array(EqualTo("other", "20250101"))
    StagePartitionUtil.parsePartitionValuesFromOverwriteFilters(filters, Array("ds")) shouldBe empty
  }
}
