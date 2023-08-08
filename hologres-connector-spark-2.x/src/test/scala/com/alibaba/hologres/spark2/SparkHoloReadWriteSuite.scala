package com.alibaba.hologres.spark2

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

class SparkHoloReadWriteSuite extends SparkHoloSuiteBase {

  def dataTypeTest(useCopy: Boolean): Unit = {
    val table = "table_for_holo_test_1"
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)

    val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
    val intArray = Array(1, 2, 3)
    val longArray = Array(1L, 2L, 3L)
    val floatArray = Array(1.2F, 2.44F, 3.77F)
    val doubleArray = Array(1.222, 2.333, 3.444)
    val booleanArray = Array(true, false, false)
    val stringArray = Array("abcd", "bcde", "defg")
    val json: String = "{\"a\":\"b\"}"
    val jsonb: String = "{\"a\": \"b\"}"
    val roaringBitmap = Array[Byte](58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0) /*{1,4,5}*/

    val data = Seq(
      Row(0L, 1.shortValue(), -7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00"),
        Date.valueOf("2021-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(1L, 2.shortValue(), 6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00"),
        Date.valueOf("1971-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(2L, 3.shortValue(), 1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, 8.9F, Timestamp.valueOf("2020-02-29 16:12:33"),
        Date.valueOf("2020-07-23"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(3L, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      defaultSchema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "insertOrIgnore")
      .option(SourceProvider.COPY_WRITE_MODE, useCopy)
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .save()

    // Read the data just written
    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .load().orderBy("pk").cache()

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  test("data type test insert.") {
    dataTypeTest(true)
  }

  test("data type test copy.") {
    dataTypeTest(false)
  }

  def partialInsertTest(useCopy: Boolean): Unit = {
    val table = "table_for_holo_test_1"
    val byteA = Array(4.toByte, 5.toByte, 6.toByte, 'q'.toByte, 'e'.toByte)
    val intA = Array(4, 5, 6)
    val doubleA = Array(2.333, 3.444, 4.555)

    val data = Seq(
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(1L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("count", IntegerType),
      StructField("name", StringType),
      StructField("thick", FloatType),
      StructField("time", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType))
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    ).orderBy("pk").cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, useCopy)
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  test("update part fields and read test copy.") {
    partialInsertTest(true);
  }

  test("update part fields and read test insert.") {
    partialInsertTest(false);
  }

  test("write or read not exists columns.") {
    val table = "table_for_holo_test_1"
    val data = Seq(
      Row(2L, -7L)
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("not_exist_column", LongType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    ).orderBy("pk").cache()

    try {
      df.write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
        .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
        .save()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column not_exist_column does not exist in hologres table table_for_holo_test_1")) {
          throw new RuntimeException(e)
        }
    }

    try {
      val readDf = spark.read
        .format("hologres")
        .schema(newSchema)
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column not_exist_column does not exist in hologres table table_for_holo_test_1")) {
          throw new RuntimeException(e)
        }
    }
  }

  test("write or read type not match.") {
    val table = "table_for_holo_test_1"
    val data = Seq(
      Row(2L, "ididid")
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", StringType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    ).orderBy("pk").cache()

    try {
      df.write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
        .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
        .save()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column id in hologres table table_for_holo_test_1 type does not match: spark type: StringType, hologres type: int8")) {
          throw new RuntimeException(e)
        }
    }

    try {
      val readDf = spark.read
        .format("hologres")
        .schema(newSchema)
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column id in hologres table table_for_holo_test_1 type does not match: spark type: StringType, hologres type: int8")) {
          throw new RuntimeException(e)
        }
    }

  }

}
