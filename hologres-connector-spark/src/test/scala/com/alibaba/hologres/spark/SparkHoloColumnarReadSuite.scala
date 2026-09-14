package com.alibaba.hologres.spark

import com.alibaba.hologres.spark.source.copy.arrow.ArrowColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class SparkHoloColumnarReadSuite extends SparkHoloSuiteBase {

  // 列式读取支持的类型子集(不含数组/json/jsonb/roaringbitmap)
  private val columnarSchema = StructType(Array(
    StructField("pk", LongType, nullable = false),
    StructField("st", ShortType),
    StructField("count", IntegerType),
    StructField("name", StringType),
    StructField("price", DecimalType(38, 12)),
    StructField("out_of_stock", BooleanType),
    StructField("weight", DoubleType),
    StructField("thick", FloatType),
    StructField("ts1", TimestampType),
    StructField("ts2", TimestampType),
    StructField("dt", DateType),
    StructField("by", BinaryType)
  ))

  private val createColumnarTableDDL: String = "create table TABLE_NAME (" +
    "    pk bigint primary key," +
    "    st smallint," +
    "    count int," +
    "    name text," +
    "    price numeric(38, 12)," +
    "    out_of_stock bool," +
    "    weight double precision," +
    "    thick float4," +
    "    ts1 timestamp," +
    "    ts2 timestamptz," +
    "    dt date," +
    "    by bytea);"

  private def prepareColumnarData(table: String): DataFrame = {
    val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
    val data = Seq(
      Row(0L, 1.shortValue(), 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F,
        Timestamp.valueOf("2021-01-01 00:00:00.123"), Timestamp.valueOf("2021-01-01 00:00:00.456"),
        Date.valueOf("2021-01-01"), byteArray),
      Row(1L, 2.shortValue(), -10, "phone2\"", BigDecimal(1234.56), true, 188.45, 7.8F,
        Timestamp.valueOf("2021-01-01 12:00:00.123"), Timestamp.valueOf("2021-01-01 00:00:00.456"),
        Date.valueOf("1971-01-01"), byteArray),
      Row(2L, null, null, null, null, null, null, null, null, null, null, null)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      columnarSchema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .mode(SaveMode.Append)
      .save()
    df
  }

  private def readTable(table: String, readMode: String, columnarEnabled: Boolean = true): DataFrame = {
    spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MODE, readMode)
      .option(SourceProvider.READ_ENABLE_COLUMNAR_READ, columnarEnabled.toString)
      .load()
  }

  // 触发执行后检查物理计划是否走了列式读取(scan输出ColumnarBatch时Spark会插入ColumnarToRow节点)
  private def isColumnarScan(df: DataFrame): Boolean = {
    df.collect()
    df.queryExecution.executedPlan.toString.contains("ColumnarToRow")
  }

  private def compareData(expected: DataFrame, actual: DataFrame): Unit = {
    if (expected.except(actual).count() > 0 || actual.except(expected).count() > 0) {
      expected.show(false)
      actual.show(false)
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  test("columnar read used for bulk_read and result is consistent.") {
    val table = "table_for_columnar_read_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(createColumnarTableDDL, table)
    val df = prepareColumnarData(table)

    // 物理计划走列式读取
    assert(isColumnarScan(readTable(table, "bulk_read")))

    // 列式读取结果与写入数据一致
    val readDf = readTable(table, "bulk_read")
    compareData(df, readDf)

    // 列式读取结果与select行式读取结果一致
    val selectDf = readTable(table, "select")
    compareData(selectDf, readDf)

    testUtils.dropTable(table)
  }

  test("columnar read disabled by read.enable_columnar_read.") {
    val table = "table_for_columnar_read_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(createColumnarTableDDL, table)
    val df = prepareColumnarData(table)

    val readDf = readTable(table, "bulk_read", columnarEnabled = false)
    // 开关关闭时回退行式读取, 数据仍一致
    assert(!isColumnarScan(readTable(table, "bulk_read", columnarEnabled = false)))
    compareData(df, readDf)

    testUtils.dropTable(table)
  }

  test("fall back to row-based read when schema contains array type.") {
    val table = "table_for_columnar_read_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)
    val df = prepareData(table)

    val readDf = readTable(table, "bulk_read")
    // 含数组等不支持类型时自动回退行式读取, 数据仍一致
    assert(!isColumnarScan(readTable(table, "bulk_read")))
    compareData(df, readDf)

    testUtils.dropTable(table)
  }

  test("count with bulk_read on columnar supported table.") {
    val table = "table_for_columnar_read_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(createColumnarTableDDL, table)
    prepareColumnarData(table)

    // 列裁剪后schema为空, 回退行式读取, count结果正确
    assert(readTable(table, "bulk_read").count() == 3)

    testUtils.dropTable(table)
  }

  // 列裁剪后 required schema 为空(count(*))时, 各读取模式都必须能正常工作:
  // 空投影 SQL 的列数对不上, 需要退化为常量投影 / 退回 select 模式
  Seq("select", "bulk_read", "bulk_read_compressed").foreach { readMode =>
    Seq(true, false).foreach { columnarEnabled =>
      test(s"count with empty required schema in $readMode mode " +
        s"(columnar=$columnarEnabled).") {
        val table = "table_for_empty_proj_" + randomSuffix
        testUtils.dropTable(table)
        testUtils.createTable(createColumnarTableDDL, table)
        prepareColumnarData(table)
        try {
          assert(readTable(table, readMode, columnarEnabled).count() == 3)
          // select 1 这种带常量投影的场景同样不消费任何表字段
          assert(readTable(table, readMode, columnarEnabled).selectExpr("1 as c")
            .count() == 3)
        } finally {
          testUtils.dropTable(table)
        }
      }
    }
  }

  test("supportColumnarReads type gating.") {
    // 全部为支持的类型
    assert(ArrowColumnVector.supportColumnarReads(columnarSchema))
    // 包含数组类型时不支持
    assert(!ArrowColumnVector.supportColumnarReads(defaultSchema))
    // 空schema不支持(如select count(*)场景)
    assert(!ArrowColumnVector.supportColumnarReads(StructType(Array.empty[StructField])))
  }
}
