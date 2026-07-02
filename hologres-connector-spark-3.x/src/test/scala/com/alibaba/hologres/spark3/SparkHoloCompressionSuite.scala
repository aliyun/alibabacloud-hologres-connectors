package com.alibaba.hologres.spark3

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.runner.RunWith
import org.scalatest.Tag
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkHoloCompressionSuite extends SparkHoloSuiteBase {

  test("stage write with arrow compression enabled.") {
    val table = "table_for_holo_compression_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.executeSql(s"CREATE TABLE $table (pk bigint primary key, id bigint, name text, empty_col text, empty_str_col text)")

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("empty_col", StringType),     // 整列全为 null
      StructField("empty_str_col", StringType)  // 整列全为空字符串 ""
    ))

    // Generate enough data to show compression difference
    // empty_col 全部为 null，验证含空列的 arrow 压缩正确性
    val data = (0 until 1000).map(i =>
      Row(i.toLong, i.toLong * 10, s"name_padding_for_compression_test_$i", null, ""))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    // 1. Write without compression, only_stage=true to keep stage for inspection
    df.coalesce(1).write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option("write.stage.only_stage", "true")
      .option("write.stage.compression", "false")
      .mode(SaveMode.Append)
      .save()

    // Query stage_bytes for uncompressed stage
    val conn1 = java.sql.DriverManager.getConnection(testUtils.jdbcUrl, testUtils.username, testUtils.password)
    val rs1 = conn1.createStatement().executeQuery("select stage_name, stage_bytes from hologres.hg_internal_stages order by create_time desc limit 1")
    rs1.next()
    val uncompressedStageName = rs1.getString("stage_name")
    val uncompressedBytes = rs1.getLong("stage_bytes")
    rs1.close()
    conn1.close()
    println(s"[Compression Test] Uncompressed stage: $uncompressedStageName, bytes: $uncompressedBytes")

    // 2. Write with compression, only_stage=true
    df.coalesce(1).write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option("write.stage.only_stage", "true")
      .option("write.stage.compression", "true")
      .mode(SaveMode.Append)
      .save()

    // Query stage_bytes for compressed stage
    val conn2 = java.sql.DriverManager.getConnection(testUtils.jdbcUrl, testUtils.username, testUtils.password)
    val rs2 = conn2.createStatement().executeQuery("select stage_name, stage_bytes from hologres.hg_internal_stages order by create_time desc limit 1")
    rs2.next()
    val compressedStageName = rs2.getString("stage_name")
    val compressedBytes = rs2.getLong("stage_bytes")
    rs2.close()
    conn2.close()
    println(s"[Compression Test] Compressed stage: $compressedStageName, bytes: $compressedBytes")
    println(s"[Compression Test] Compression ratio: ${uncompressedBytes.toDouble / compressedBytes.toDouble}")

    // Compressed should be smaller
    assert(compressedBytes < uncompressedBytes, s"compressed($compressedBytes) should < uncompressed($uncompressedBytes)")

    // 3. Also verify compressed stage can be committed and data is correct
    df.coalesce(1).write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option("write.stage.compression", "true")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .load().orderBy("pk").cache()

    assert(readDf.count() == 1000, s"expected 1000 rows, got ${readDf.count()}")


    testUtils.dropTable(table)
  }
}
