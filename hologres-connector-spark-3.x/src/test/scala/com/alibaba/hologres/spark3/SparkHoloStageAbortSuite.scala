package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.copy.StageWriterCommitMessage
import com.alibaba.hologres.spark.utils.{JDBCUtil, SparkHoloUtil}
import com.alibaba.hologres.spark3.sink.HoloBatchWriter
import com.alibaba.hologres.spark3.sink.copy.HoloDataStageWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.sql.DriverManager

@RunWith(classOf[JUnitRunner])
class SparkHoloStageAbortSuite extends SparkHoloSuiteBase {

  private def stageExists(stageName: String): Boolean = {
    val conn = DriverManager.getConnection(testUtils.jdbcUrl, testUtils.username, testUtils.password)
    try {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(s"select 1 from hologres.hg_internal_stages where stage_name = '$stageName'")
      val exists = rs.next()
      rs.close()
      stmt.close()
      exists
    } finally {
      conn.close()
    }
  }

  private def createStageWriteConfig(table: String): HologresConfigs = {
    new HologresConfigs(Map(
      "username" -> testUtils.username,
      "password" -> testUtils.password,
      "jdbcurl" -> testUtils.jdbcUrl,
      "table" -> table,
      "write.mode" -> "stage"
    ))
  }

  test("stage task writer abort drops stage") {
    val table = "table_for_holo_stage_task_abort_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.executeSql(s"CREATE TABLE $table (pk bigint primary key, id bigint, name text)")

    val conf = createStageWriteConfig(table)
    val (holoSchema, _) = SparkHoloUtil.getHoloSchema(conf)
    val sparkSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("name", StringType)
    ))

    val stageName = s"${table}_task_abort_test"
    JDBCUtil.createStage(conf, stageName)
    assert(stageExists(stageName), s"stage $stageName should exist before abort")

    val writer = new HoloDataStageWriter(conf, sparkSchema, holoSchema, stageName, taskId = "0")
    writer.write(InternalRow(1L, 10L, UTF8String.fromString("a")))
    writer.write(InternalRow(2L, 20L, UTF8String.fromString("b")))

    // abort should close wrapper and drop stage
    writer.abort()

    assert(!stageExists(stageName), s"stage $stageName should be dropped after task writer abort")

    testUtils.dropTable(table)
  }

  test("stage batch writer abort drops stages from commit messages") {
    val table = "table_for_holo_stage_batch_abort_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.executeSql(s"CREATE TABLE $table (pk bigint primary key, id bigint, name text)")

    val conf = createStageWriteConfig(table)
    val (holoSchema, _) = SparkHoloUtil.getHoloSchema(conf)
    val sparkSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("name", StringType)
    ))

    val stageName1 = s"${table}_batch_abort_1"
    val stageName2 = s"${table}_batch_abort_2"
    JDBCUtil.createStage(conf, stageName1)
    JDBCUtil.createStage(conf, stageName2)
    assert(stageExists(stageName1) && stageExists(stageName2), "stages should exist before abort")

    val batchWriter = new HoloBatchWriter(conf, sparkSchema, is_overwrite = false)
    batchWriter.abort(Array(
      StageWriterCommitMessage(stageName1),
      StageWriterCommitMessage(stageName2)
    ))

    assert(!stageExists(stageName1), s"stage $stageName1 should be dropped after batch abort")
    assert(!stageExists(stageName2), s"stage $stageName2 should be dropped after batch abort")

    testUtils.dropTable(table)
  }

  test("stage batch writer abort handles null and non-stage messages") {
    val table = "table_for_holo_stage_batch_abort_mixed_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.executeSql(s"CREATE TABLE $table (pk bigint primary key, id bigint, name text)")

    val conf = createStageWriteConfig(table)
    val (holoSchema, _) = SparkHoloUtil.getHoloSchema(conf)
    val sparkSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("name", StringType)
    ))

    val stageName = s"${table}_batch_abort_mixed"
    JDBCUtil.createStage(conf, stageName)
    assert(stageExists(stageName), s"stage $stageName should exist before abort")

    val batchWriter = new HoloBatchWriter(conf, sparkSchema, is_overwrite = false)
    // mix null message, stage message and a dummy non-stage message
    batchWriter.abort(Array(null, StageWriterCommitMessage(stageName), new org.apache.spark.sql.connector.write.WriterCommitMessage {}))

    assert(!stageExists(stageName), s"stage $stageName should be dropped after batch abort with mixed messages")

    testUtils.dropTable(table)
  }
}
