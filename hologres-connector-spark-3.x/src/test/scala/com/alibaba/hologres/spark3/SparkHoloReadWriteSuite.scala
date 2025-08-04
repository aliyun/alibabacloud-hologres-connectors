package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.utils.RepartitionUtil
import com.alibaba.hologres.spark.{ReadType, WriteType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

import java.sql.{Date, Timestamp}

class SparkHoloReadWriteSuite extends SparkHoloSuiteBase {

  def dataTypeTest(readType: ReadType.Value, writeType: WriteType.Value, querySource: Boolean = false,
                   skipJsonb: Boolean = false, useAkv4: Boolean = false): Unit = {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    if (skipJsonb) {
      // a hack to skip jsonb type test
      val ddl = defaultCreateHoloTableDDL.replace("jsonb_column jsonb", "jsonb_column json")
      testUtils.createTable(ddl, table)
    } else {
      testUtils.createTable(defaultCreateHoloTableDDL, table)
    }

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
      Row(0L, 1.shortValue(), -7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2021-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(1L, 2.shortValue(), 6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("1971-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(2L, 3.shortValue(), 1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, 8.9F, Timestamp.valueOf("2020-02-29 16:12:33.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2020-07-23"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(3L, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null)
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
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .option(SourceProvider.WRITE_MODE, writeType.toString)
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .option(SourceProvider.ENABLE_AKV4, useAkv4)
      .option(SourceProvider.AKV4_REGION, "cn-beijing")
      .mode(SaveMode.Append)
      .save()

    val sourceKey: String = if (querySource) SourceProvider.READ_QUERY else SourceProvider.TABLE
    val sourceValue: String = if (querySource) "select * from " + table else table
    // Read the data just written
    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(sourceKey, sourceValue)
      .option(SourceProvider.READ_MODE, readType.toString)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .option(SourceProvider.ENABLE_AKV4, useAkv4)
      .option(SourceProvider.AKV4_REGION, "cn-beijing")
      .load().orderBy("pk").cache()

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show(false)
      readDf.show(false)
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("data type test.") {
    log.info("insert then select.")
    dataTypeTest(ReadType.SELECT, WriteType.INSERT)

    log.info("fixed_copy then select.")
    dataTypeTest(ReadType.SELECT, WriteType.STREAM)

    log.info("bulk_load then select.")
    dataTypeTest(ReadType.SELECT, WriteType.BULK_LOAD)

    log.info("fixed_copy then select, and use a query source.")
    dataTypeTest(ReadType.SELECT, WriteType.STREAM, querySource = true)

    log.info("fixed_copy then copy out with arrow format, not support jsonb now.")
    dataTypeTest(ReadType.BULK_READ, WriteType.STREAM, skipJsonb = true)

    log.info("fixed_copy then copy out with arrow format, use a query source, not support jsonb now.")
    dataTypeTest(ReadType.BULK_READ, WriteType.STREAM, querySource = true, skipJsonb = true)

    log.info("auto write then auto read, not support jsonb now.")
    dataTypeTest(ReadType.AUTO, WriteType.AUTO, skipJsonb = true)

    log.info("insert then select.")
    dataTypeTest(ReadType.SELECT, WriteType.INSERT, useAkv4 = true)

    log.info("fixed_copy then select.")
    dataTypeTest(ReadType.SELECT, WriteType.STREAM, useAkv4 = true)

    log.info("bulk_load then select.")
    dataTypeTest(ReadType.SELECT, WriteType.BULK_LOAD, useAkv4 = true)
  }

  def partialInsertTest(copyMode: String, optionsMap: Map[String, String] = Map.empty): Unit = {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table, hasPk = false)
    prepareData(table)
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
      StructField("NAME", StringType),
      StructField("thick", FloatType),
      StructField("ts1", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType))
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    ).orderBy("pk").cache()

    val writer = df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, copyMode)
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Append)
    optionsMap.foreach { case (key, value) => writer.option(key, value) }
    writer.save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("update part fields and read test copy.") {
    partialInsertTest("stream")
  }

  test("update part fields and read test insert.") {
    partialInsertTest("insert")
  }

  test("update part fields and read test bulk load.") {
    partialInsertTest("bulk_load_on_conflict")
    partialInsertTest("bulk_load_on_conflict", Map{SourceProvider.WRITE_COPY_DISABLE_RIGHT_JOIN -> true.toString})
  }

  test("SaveMode = overwrite.") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table, hasPk = false)

    val byteA = Array(4.toByte, 5.toByte, 6.toByte, 'q'.toByte, 'e'.toByte)
    val intA = Array(4, 5, 6)
    val doubleA = Array(2.333, 3.444, 4.555)

    val data1 = Seq(
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(1L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val data2 = Seq(
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(1L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA),
      Row(1L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("count", IntegerType),
      StructField("NAME", StringType),
      StructField("thick", FloatType),
      StructField("ts1", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType))
    ))

    var df = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      newSchema
    ).orderBy("pk").cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, "bulk_load")
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    df = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      newSchema
    ).orderBy("pk").cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, "bulk_load")
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .load().orderBy("pk").cache()

    assert(df.count() == 4)
    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("SaveMode = overwrite child table with schema") {
    val parentTable = "test.\"Table-Parent\""
    val partitionValue = "20240527"
    val table = "test.\"Table-Child_20240527\""

    testUtils.dropTable(parentTable)
    testUtils.createSchema("test")
    testUtils.createPartitionTable(defaultCreateHoloParentTableDDL, parentTable, table, partitionValue)

    val byteA = Array(4.toByte, 5.toByte, 6.toByte, 'q'.toByte, 'e'.toByte)
    val intA = Array(4, 5, 6)
    val doubleA = Array(2.333, 3.444, 4.555)
    val date = Date.valueOf("2024-05-27")

    val data1 = Seq(
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA, date),
      Row(1L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA, date)
    )

    val data2 = Seq(
      Row(0L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA, date),
      Row(1L, -7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA, date),
      Row(2L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA, date),
      Row(3L, 6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA, date)
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("count", IntegerType),
      StructField("NAME", StringType),
      StructField("thick", FloatType),
      StructField("ts1", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType)),
      StructField("dt", DateType)
    ))

    var df = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      newSchema
    ).orderBy("pk").cache()
    RepartitionUtil.reShuffleThenWrite(df, testUtils.username, testUtils.password, testUtils.jdbcUrl, table, saveMode = SaveMode.Overwrite)


    df = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      newSchema
    ).orderBy("pk").cache()

    RepartitionUtil.reShuffleThenWrite(df, testUtils.username, testUtils.password, testUtils.jdbcUrl, table, saveMode = SaveMode.Overwrite)

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .load().orderBy("pk").cache()

    assert(df.count() == 4)
    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  def noStrictDataTypeCheckTest(writeType: WriteType.Value, negative: Boolean = false): Unit = {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDLPrecisionPromotion, table)

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
      Row(0L, 1.shortValue(), -7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2021-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(1L, 2.shortValue(), 6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("1971-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(2L, 3.shortValue(), 1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, 8.9F, Timestamp.valueOf("2020-02-29 16:12:33.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2020-07-23"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(3L, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null)
    )

    var df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      defaultSchema
    ).cache()

    try {
      df.write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
        .option(SourceProvider.WRITE_MODE, writeType.toString)
        .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
        .option(SourceProvider.WRITE_ENABLE_STRICT_DATATYPE_CHECK, if (negative) "true" else "false")
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("type does not match: spark type: ShortType, hologres type: int8")) {
          throw new RuntimeException(e)
        }
        testUtils.dropTable(table)
        return
    }

    val sourceKey: String = SourceProvider.TABLE
    val sourceValue: String = table
    // Read the data just written
    var readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(sourceKey, sourceValue)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .load().orderBy("pk").cache()

    df = df.withColumn("pk", df("pk").cast(LongType))
      .withColumn("st", df("st").cast(LongType))
      .withColumn("id", df("id").cast(LongType))
      .withColumn("count", df("count").cast(LongType))
      .withColumn("price", df("price").cast(StringType))
      .withColumn("out_of_stock", df("out_of_stock").cast(StringType))
      .withColumn("weight", df("weight").cast(StringType))
      .withColumn("thick", df("thick").cast(DoubleType))
      .withColumn("ts1", df("ts1").cast(StringType))
      .withColumn("ts2", df("ts2").cast(StringType))
      .withColumn("dt", df("dt").cast(StringType))
      .withColumn("by", df("by").cast(BinaryType))
      .withColumn("inta", df("inta").cast(ArrayType(IntegerType)))

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show(false)
      readDf.show(false)
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("no strict cast data types") {
    log.info("implicit cast data types: insert")
    noStrictDataTypeCheckTest(WriteType.INSERT)

    log.info("implicit cast data types: stream")
    noStrictDataTypeCheckTest(WriteType.STREAM)

    log.info("implicit cast data types: bulk_load")
    noStrictDataTypeCheckTest(WriteType.BULK_LOAD)

    log.info("implicit cast data types: negative")
    noStrictDataTypeCheckTest(WriteType.BULK_LOAD, negative = true)
  }

  test("record have u0000.") {
    log.info("u0000: stream")
    testU0000(WriteType.STREAM)

    log.info("u0000: bulk_load")
    testU0000(WriteType.BULK_LOAD)

    log.info("u0000: insert")
    testU0000(WriteType.INSERT)
  }

  def testU0000(writeType: WriteType.Value): Unit = {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table, hasPk = false)

    val json: String = "{\"a\":\"\u0000b\"}"
    val jsonb: String = "{\"\u0000a\": \"b\"}"

    val data = Seq(
      Row(0L, "phone1\u0000", json, jsonb),
      Row(1L, "phone\u00002", json, jsonb)
    )

    val dataRemovedU0000 = Seq(
      Row(0L, "phone1", json.replaceAll("\u0000", ""), jsonb.replaceAll("\u0000", "")),
      Row(1L, "phone2", json.replaceAll("\u0000", ""), jsonb.replaceAll("\u0000", ""))
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("NAME", StringType),
      StructField("json_column", StringType),
      StructField("jsonb_column", StringType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    ).orderBy("pk").cache()

    val dfRemovedU0000 = spark.createDataFrame(
      spark.sparkContext.parallelize(dataRemovedU0000),
      newSchema
    ).orderBy("pk").cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, writeType.toString)
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .option(SourceProvider.READ_MODE, "select")
      .load().orderBy("pk").cache()

    assert(df.count() == 2)
    // compare read and write
    if (dfRemovedU0000.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("read from a query with join.") {
    val table1 = "table_for_holo_test_read_1"
    val table2 = "table_for_holo_test_read_2"
    testUtils.dropTable(table1)
    testUtils.dropTable(table2)
    testUtils.createTable(defaultCreateHoloTableDDL, table1, hasPk = true)
    testUtils.createTable(defaultCreateHoloTableDDL, table2, hasPk = true)

    val byteA = Array(4.toByte, 5.toByte, 6.toByte, 'q'.toByte, 'e'.toByte)
    val intA = Array(4, 5, 6)
    val doubleA = Array(2.333, 3.444, 4.555)

    val data1 = Seq(
      Row(0L, 100L, 10, "phone100", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(1L, 200L, 20, "phone200", 7.8F, Timestamp.valueOf("2022-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val data2 = Seq(
      Row(0L, 300L, -30, "phone300", 8.9F, Timestamp.valueOf("2023-03-29 00:00:00"), byteA, intA, doubleA),
      Row(1L, 400L, -40, "phone400", 9.0F, Timestamp.valueOf("2024-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val newSchema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("count", IntegerType),
      StructField("NAME", StringType),
      StructField("thick", FloatType),
      StructField("ts1", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType))
    ))

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      newSchema
    ).orderBy("pk").cache()

    df1.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table1)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, "stream")
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(data2),
      newSchema
    ).orderBy("pk").cache()

    df2.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table2)
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
      .option(SourceProvider.WRITE_MODE, "stream")
      .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    var joinDf = df1.join(df2, "pk").select(
      df1("pk").alias("pk"),
      df1("id").alias("id"),
      df2("count").alias("count"),
      df1("NAME").alias("NAME"),
      df2("thick").alias("thick"),
      df1("ts1").alias("ts1"),
      df2("by").alias("by"),
      df2("inta").alias("inta"),
      df2("doublea").alias("doublea")
    )

    var readDf = spark.read
      .format("hologres")
      // .schema(newSchema) // 读取全部字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.READ_QUERY, String.format("select t1.pk, t1.id, t2.count, t1.\"NAME\", t2.thick, t1.ts1, " +
        "t2.by, t2.inta, t2.doublea from %s t1 join %s t2 on t1.pk = t2.pk", table1, table2))
      .load().orderBy("pk").cache()

    assert(readDf.count() == 2)
    // compare read and write
    if (joinDf.except(readDf).count() > 0) {
      joinDf.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }

    // select 部分字段
    joinDf = df1.join(df2, "pk").select(
      df1("pk").alias("pk"),
      df1("id").alias("id"),
      df2("count").alias("count"),
      df1("NAME").alias("NAME"),
      df2("thick").alias("thick"),
      df1("ts1").alias("ts1")
    )
    val partsSchema = StructType(newSchema.fields
      .filterNot(_.name == "by")
      .filterNot(_.name == "inta")
      .filterNot(_.name == "doublea")
    )

    readDf = spark.read
      .format("hologres")
      .schema(partsSchema) // 指定部分字段进行读取
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.READ_QUERY, String.format("select t1.pk, t1.id, t2.count, t1.\"NAME\", t2.thick, t1.ts1, " +
        "t2.by, t2.inta, t2.doublea from %s t1 join %s t2 on t1.pk = t2.pk", table1, table2))
      .load().orderBy("pk").cache()

    assert(readDf.count() == 2)
    // compare read and write
    if (joinDf.except(readDf).count() > 0) {
      joinDf.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  test("read from a table with filters and limit") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)
    val df = prepareData(table)

    // Read the data just written
    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MODE, "select")
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .option(SourceProvider.READ_PUSH_DOWN_PREDICATE, value = true)
      .option(SourceProvider.READ_PUSH_DOWN_LIMIT, value = true)
      .load().filter("pk < 2 and cast(st as int) > cast(0 as int) and id is not null and NAME = 'phone1' " +
        "and price between cast(1234.0 as decimal(38,12)) AND cast(1234.8 as decimal(38,12)) " +
        "and out_of_stock is false and weight <> 100 and ts1 >= '2021-01-01 00:00:00' and dt < now()" +
        "and array_contains(inta, 3)").limit(5).orderBy("pk").cache()

    // compare read and write
    if (df.filter("pk = 0").except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
  }

  test("write or read not exists columns.") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)
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
        .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
        .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
        .mode(SaveMode.Append)
        .save()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column not_exist_column does not exist in hologres table table_for_holo_test_")) {
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
        .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column not_exist_column does not exist in hologres table table_for_holo_test_")) {
          throw new RuntimeException(e)
        }
    }
    testUtils.dropTable(table)
  }

  test("write or read type not match.") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)
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
        .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
        .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
        .save()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains(s"column id in hologres table ${table} type does not match: spark type: StringType, hologres type: int8")) {
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
        .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains(s"column id in hologres table ${table} type does not match: spark type: StringType, hologres type: int8")) {
          throw new RuntimeException(e)
        }
    }
    testUtils.dropTable(table)
  }

  test("read params illegal.") {
    val table = "table_for_holo_test_" + randomSuffix
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
      val readDf = spark.read
        .format("hologres")
        .schema(newSchema)
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.READ_QUERY, "select * from " + table)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("If query is provided, please do not provide parameter 'table'")) {
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
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("Missing necessary parameter 'table'. If table is not provided, please provide parameter 'query' for read")) {
          throw new RuntimeException(e)
        }
    }
  }
}
