package com.alibaba.hologres.spark

import com.alibaba.hologres.client.model.{HoloVersion, TableName}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{RepartitionUtil, SparkHoloUtil}
import com.alibaba.hologres.spark.{ReadType, WriteType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class SparkHoloReadWriteSuite extends SparkHoloSuiteBase {

  def dataTypeTest(readType: ReadType.Value, writeType: WriteType.Value, querySource: Boolean = false,
                   skipJsonb: Boolean = false, useAkv4: Boolean = false,
                   akv4RegionOverride: String = null): Unit = {
    // useAkv4 时 region 必须与实例真实 region 一致, 不能写死;
    // 不启用 akv4 时给空串而非 null, 避免往 Spark 的 options map 里塞 null
    val resolvedAkv4Region: String =
      if (!useAkv4) ""
      else Option(akv4RegionOverride).orElse(akv4Region).getOrElse(
        throw new IllegalStateException("AKv4 region unresolved; caller should have skipped"))
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
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("1900-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(1L, 2.shortValue(), 6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("1971-01-01"), byteArray,
        intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(2L, 3.shortValue(), 1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, 8.9F, Timestamp.valueOf("2020-02-29 16:12:33.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("3999-07-23"), byteArray,
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
      .option(SourceProvider.AKV4_REGION, resolvedAkv4Region)
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
      .option(SourceProvider.AKV4_REGION, resolvedAkv4Region)
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

    log.info("fixed_copy then copy out with arrow format.")
    dataTypeTest(ReadType.BULK_READ, WriteType.STREAM)

    log.info("fixed_copy then copy out with arrow format, use a query source, not support jsonb now.")
    dataTypeTest(ReadType.BULK_READ, WriteType.STREAM, querySource = true, skipJsonb = true)

    log.info("auto write then auto read, not support jsonb now.")
    dataTypeTest(ReadType.AUTO, WriteType.AUTO)

    // AKv4 场景只在能确定实例 region 时才跑: region 参与签名密钥派生, 填错会直接认证失败,
    // 而裸 IP 的本地/内网实例无法从 host 推断 region. 需要时用 HOLO_AKV4_REGION 显式指定.
    if (akv4Region.isDefined) {
      log.info(s"insert then select, akv4 region=${akv4Region.get}.")
      dataTypeTest(ReadType.SELECT, WriteType.INSERT, useAkv4 = true)

      log.info("fixed_copy then select, akv4.")
      dataTypeTest(ReadType.SELECT, WriteType.STREAM, useAkv4 = true)

      log.info("bulk_load then select, akv4.")
      dataTypeTest(ReadType.SELECT, WriteType.BULK_LOAD, useAkv4 = true)
    } else {
      log.warn("skip AKv4 scenarios: cannot resolve instance region from " +
        s"jdbcUrl ${testUtils.jdbcUrl}; set HOLO_AKV4_REGION to enable them.")
    }

    log.info("insert from stage then select.")
    dataTypeTest(ReadType.SELECT, WriteType.STAGE, useAkv4 = false)
  }

  test("timestamp precision and pre-1970 test.") {
    // 1970 年之前的负值向零截断(micros/1000)会导致时间偏移, 亚毫秒精度在转换中丢失,
    // 覆盖写(FieldWriter)/arrow读(SparkArrowTimeStampMicroAccessor)/select读(TimestampFieldReader)各路径.
    log.info("insert then select.")
    timestampPrecisionTest(ReadType.SELECT, WriteType.INSERT)

    log.info("fixed_copy then select.")
    timestampPrecisionTest(ReadType.SELECT, WriteType.STREAM)

    log.info("bulk_load then select.")
    timestampPrecisionTest(ReadType.SELECT, WriteType.BULK_LOAD)

    log.info("fixed_copy then copy out with arrow format.")
    timestampPrecisionTest(ReadType.BULK_READ, WriteType.STREAM)

    log.info("insert from stage then select.")
    timestampPrecisionTest(ReadType.SELECT, WriteType.STAGE)
  }

  def timestampPrecisionTest(readType: ReadType.Value, writeType: WriteType.Value): Unit = {
    val table = "table_for_holo_ts_precision_test_" + randomSuffix
    testUtils.dropTable(table)
    // 注: 不覆盖 timestamptz 列. holo-client 写 timestamptz 时按 UTC wall-clock 处理(未做会话时区转换),
    // 与读侧不对称导致往返偏移, 属 holo-client 既有问题, 另行跟进.
    // 1900 年等早期时间写入会带 IANA 历史时区(LMT)偏移, 但读侧对称还原, 往返一致.
    val ddl =
      s"""CREATE TABLE $table (
         |pk bigint NOT NULL,
         |ts timestamp,
         |dt date,
         |PRIMARY KEY (pk)
         |)""".stripMargin
    testUtils.createTable(ddl, table)

    def tsWithMicros(str: String, micros: Int): Timestamp = {
      val ts = Timestamp.valueOf(str)
      ts.setNanos(micros * 1000)
      ts
    }

    val data = Seq(
      // 1970 之前且带亚毫秒: 负值截断 + 精度丢失的双重边界
      Row(0L, tsWithMicros("1969-12-31 23:59:59", 999999), Date.valueOf("1969-12-31")),
      // 1970 之前整秒(含历史时区 LMT 偏移场景, 往返应一致)
      Row(1L, Timestamp.valueOf("1900-01-01 00:00:00"), Date.valueOf("1900-01-01")),
      // 现代时间带微秒精度
      Row(2L, tsWithMicros("2023-11-15 06:13:20", 123456), Date.valueOf("2023-11-15")),
      // 1970 之后仅毫秒
      Row(3L, Timestamp.valueOf("2021-01-01 00:00:00.123"), Date.valueOf("1971-01-01"))
    )

    val schema = StructType(Array(
      StructField("pk", LongType, nullable = false),
      StructField("ts", TimestampType),
      StructField("dt", DateType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, writeType.toString)
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.READ_MODE, readType.toString)
      .option(SourceProvider.READ_MAX_TASK_COUNT, 4)
      .load().orderBy("pk").cache()

    if (df.except(readDf).count() > 0) {
      df.show(false)
      readDf.show(false)
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
    testUtils.dropTable(table)
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
    partialInsertTest("bulk_load_on_conflict", Map {
      SourceProvider.WRITE_COPY_DISABLE_RIGHT_JOIN -> true.toString
    })
  }

  test("update part fields and read test stage.") {
    partialInsertTest("stage")
  }

  test("SaveMode = overwrite.") {
    overwrite("bulk_load")
    overwrite("stage")
  }

  def overwrite(copyMode: String) {
    val table = "table_for_holo_test_" + copyMode + randomSuffix
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
      .option(SourceProvider.WRITE_MODE, copyMode)
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
      .option(SourceProvider.WRITE_MODE, copyMode)
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
    val suffix = randomSuffix
    val parentTable = s"""test."Table-Parent_$suffix""""
    val partitionValue = "20240527"
    val table = s"""test."Table-Child_20240527_$suffix""""

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
    testUtils.dropTable(parentTable)
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
    val suffix = randomSuffix
    val table1 = "table_for_holo_test_read_1_" + suffix
    val table2 = "table_for_holo_test_read_2_" + suffix
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
    testUtils.dropTable(table1)
    testUtils.dropTable(table2)
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

  test("write with rate limit.") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)

    // Generate 10000 rows with pk only
    val data = (0 until 100).map(i => Row(i.toLong))

    val newSchema = StructType(Array(
      StructField("pk", LongType)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    )

    for (write_mode <- Seq("auto", "stream", "bulk_load_on_conflict", "insert", "stage")) {
      val startTime = System.currentTimeMillis()
      df.coalesce(1).write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.WRITE_MODE, write_mode)
        .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrUpdate")
        .option(SourceProvider.WRITE_COPY_DIRTY_DATA_CHECK, "true")
        .option(SourceProvider.WRITE_RPS_LIMIT, 10)
        .mode(SaveMode.Append)
        .save()
      val elapsed = System.currentTimeMillis() - startTime
      // 100 rows at 10 rps should take ~10 seconds
      assert(elapsed >= 8000, s"Rate limit not working, elapsed: ${elapsed}ms")
    }

    testUtils.dropTable(table)
  }

  // -------------------- Stage partition INSERT tests --------------------

  // COPY 类模式的 PARTITION 子句需要 Hologres >= 5.0, 低版本实例(如线上 CI 的 4.2)跳过 COPY 分区用例;
  // stage 模式分区写入为通用 SQL 能力, 不依赖实例版本, 无需守卫
  def requireCopyPartitionSupport(): Unit = {
    val version = testUtils.getHoloVersion
    if (version.compareTo(new HoloVersion(5, 0, 0)) < 0) {
      cancel(s"Skip COPY mode partition tests: requires Hologres >= 5.0, but current instance is $version")
    }
  }

  /**
   * Create a logical partition parent table with child partitions and write via stage mode
   * with write.target_partition_columns / write.target_partition_values options.
   */
  test("stage write with logical partition INSERT.") {
    stagePartitionInsertSinglePartition()
    stagePartitionInsertMultiplePartitions()
    stagePartitionInsertMultiColumns()
    stagePartitionInsertFullyAuto()
    stagePartitionConfigMustBeSetTogether()
    nonStageOverwriteLogicalPartitionMustFail()
    stagePartitionOverwriteDeclaredPartitions()
    stagePartitionOverwriteFullyAuto()
  }

  test("copy mode write with logical partition INSERT.") {
    requireCopyPartitionSupport()
    copyPartitionInsert("bulk_load")
    copyPartitionInsert("stream")
  }

  test("stage write with logical partition column types and boundaries.") {
    stagePartitionIntColumn()
    stagePartitionDateColumn()
    stagePartitionTimestampColumn()
    stagePartitionTextSpecialChars()
  }

  /**
   * Hologres >= 4.2.16, 或 4.1.x 分支 >= 4.1.35 时 write.mode=auto 默认选择 stage 模式,
   * 不再进入 bulk_load / reshuffle(V1) 路径. 低版本实例跳过.
   */
  test("write mode auto defaults to stage on supported holo versions.") {
    val version = testUtils.getHoloVersion
    val supportDefaultStage = version.compareTo(new HoloVersion(4, 2, 16)) >= 0 ||
      (version.getMajorVersion == 4 && version.getMinorVersion == 1 &&
        version.compareTo(new HoloVersion(4, 1, 35)) >= 0)
    if (!supportDefaultStage) {
      cancel(s"Skip auto->stage default test: requires Hologres >= 4.2.16 or 4.1.x >= 4.1.35, but current instance is $version")
    }
    val table = "table_for_holo_auto_stage_" + randomSuffix
    testUtils.executeSql(s"DROP TABLE IF EXISTS $table CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $table (
         |  id bigint not null,
         |  name text,
         |  PRIMARY KEY(id)
         |)""".stripMargin)
    try {
      val sparkSchema = StructType(Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType)
      ))
      val configs = new HologresConfigs(Map(
        "username" -> testUtils.username,
        "password" -> testUtils.password,
        "jdbcurl" -> testUtils.jdbcUrl,
        "table" -> table
      ))
      val holoSchema = testUtils.client.getTableSchema(TableName.valueOf(table))
      val chosen = SparkHoloUtil.chooseBestMode(sparkSchema, holoSchema, configs)
      assert("stage" == chosen.writeMode,
        s"write.mode=auto should default to stage on Hologres >= 4.2.16 or 4.1.x >= 4.1.35, but got ${chosen.writeMode}")

      // 不显式指定 write.mode 实际写入并读回验证
      val sparkSession = spark
      import sparkSession.implicits._
      val df = Seq((1L, "auto-stage")).toDF("id", "name")
      df.write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .mode(SaveMode.Append)
        .save()
      val readDf = spark.read
        .format("hologres")
        .schema(sparkSchema)
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .load()
      assert(readDf.count() == 1)
    } finally {
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table CASCADE")
    }
  }

  /** stage 全自动模式写入(不配置分区参数), 分区列从表元数据解析, 分区值从数据采集. */
  private def writeStageFullyAuto(df: DataFrame, table: String, mode: SaveMode): Unit = {
    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .mode(mode)
      .save()
  }

  /** 读回全表与期望数据比对. */
  private def verifyTableData(expected: DataFrame, table: String, schema: StructType, label: String): Unit = {
    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .load().cache()
    val expectedRows = expected.collect()
    assert(readDf.count() == expectedRows.length,
      s"$label: expect ${expectedRows.length} rows but got ${readDf.count()}")
    if (expected.except(readDf).count() > 0) {
      expected.show(false)
      readDf.show(false)
      throw new Exception(s"$label: data mismatch!")
    }
  }

  def stagePartitionInsertSinglePartition(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_test_$suffix"

    // cleanup
    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    // create LOGICAL partition parent table (not physical partition)
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  "NAME" text,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("NAME", StringType),
      StructField("ds", StringType)
    ))

    // Write data for partition ds='20250101' only
    val data = Seq(
      Row(1L, 100L, "phone1", "20250101"),
      Row(2L, 200L, "phone2", "20250101")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .option(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS, "\"ds\"")
      .option(SourceProvider.WRITE_TARGET_PARTITION_VALUES, "\"20250101\"")
      .mode(SaveMode.Append)
      .save()

    // Read back and verify
    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    assert(readDf.count() == 2)
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("stage partition INSERT: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  def stagePartitionInsertMultiplePartitions(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_multi_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    // create LOGICAL partition parent table
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  "NAME" text,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("NAME", StringType),
      StructField("ds", StringType)
    ))

    // Write data for both partitions
    val data = Seq(
      Row(1L, 100L, "phone1", "20250101"),
      Row(2L, 200L, "phone2", "20250101"),
      Row(3L, 300L, "phone3", "20250102"),
      Row(4L, 400L, "phone4", "20250102")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .option(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS, "\"ds\"")
      .option(SourceProvider.WRITE_TARGET_PARTITION_VALUES, "\"20250101\"; \"20250102\"")
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    assert(readDf.count() == 4)
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("stage partition INSERT multi-partition: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  def stagePartitionInsertMultiColumns(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_multi_col_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    // LOGICAL partition by (ds, kind)
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  kind text NOT NULL,
         |  PRIMARY KEY(pk, ds, kind)
         |) LOGICAL PARTITION BY LIST(ds, kind)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType),
      StructField("kind", StringType)
    ))

    val data = Seq(
      Row(1L, 100L, "20250101", "a"),
      Row(2L, 200L, "20250101", "a"),
      Row(3L, 300L, "20250102", "b")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .option(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS, "\"ds\", \"kind\"")
      .option(SourceProvider.WRITE_TARGET_PARTITION_VALUES, "\"20250101\", \"a\"; \"20250102\", \"b\"")
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    assert(readDf.count() == 3)
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("stage partition INSERT multi-column: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * 不配置任何分区参数(stage 全自动模式): 分区列从表元数据解析,
   * 分区值从实际写入的数据中自动采集.
   */
  def stagePartitionInsertFullyAuto(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_auto_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))

    val data = Seq(
      Row(1L, 100L, "20250101"),
      Row(2L, 200L, "20250102"),
      Row(3L, 300L, "20250103")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    // 不配置任何分区参数: 分区列由表元数据解析, 分区值由writer从数据自动采集
    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_ON_CONFLICT_ACTION, "insertOrIgnore")
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    assert(readDf.count() == 3)
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("stage partition INSERT fully auto: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * write.target_partition_columns / write.target_partition_values 必须成对设置,
   * 只设置其中一个时应报错.
   */
  def stagePartitionConfigMustBeSetTogether(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_pair_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L, 100L, "20250101"))), schema
    ).cache()

    def writeWith(options: Map[String, String]): Unit = {
      var writer = df.write
        .format("hologres")
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, parentTable)
        .option(SourceProvider.WRITE_MODE, "stage")
      options.foreach { case (k, v) => writer = writer.option(k, v) }
      writer.mode(SaveMode.Append).save()
    }

    // 只设 columns
    val e1 = intercept[IllegalArgumentException] {
      writeWith(Map(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS -> "\"ds\""))
    }
    assert(e1.getMessage.contains("must be set together"),
      s"unexpected message: ${e1.getMessage}")

    // 只设 values
    val e2 = intercept[IllegalArgumentException] {
      writeWith(Map(SourceProvider.WRITE_TARGET_PARTITION_VALUES -> "\"20250101\""))
    }
    assert(e2.getMessage.contains("must be set together"),
      s"unexpected message: ${e2.getMessage}")

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * 非 stage 模式 overwrite 逻辑分区表必须被拦截:
   * 非 stage 的 overwrite 走临时表+rename 全表替换, 会清掉逻辑分区表所有分区的数据.
   */
  def nonStageOverwriteLogicalPartitionMustFail(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_ovw_guard_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L, 100L, "20250101"))), schema
    ).cache()

    // Spark 可能包装异常, 沿 cause 链拼接所有 message
    def causeMessages(e: Throwable): String =
      Iterator.iterate(e)(_.getCause).takeWhile(_ != null)
        .map(_.getMessage).mkString(" | ")

    for (writeMode <- Seq("insert", "stream", "bulk_load")) {
      val e = intercept[Exception] {
        df.write
          .format("hologres")
          .option(SourceProvider.USERNAME, testUtils.username)
          .option(SourceProvider.PASSWORD, testUtils.password)
          .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
          .option(SourceProvider.TABLE, parentTable)
          .option(SourceProvider.WRITE_MODE, writeMode)
          .mode(SaveMode.Overwrite)
          .save()
      }
      assert(causeMessages(e).contains("only supported in stage write mode"),
        s"write.mode=$writeMode overwrite should be rejected for logical partition table, " +
          s"but got: ${causeMessages(e)}")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * SaveMode.Overwrite 且显式声明分区: 按声明字面执行,
   * 声明了但无数据的分区也会被清空(与 Hive INSERT OVERWRITE PARTITION 语义一致).
   */
  def stagePartitionOverwriteDeclaredPartitions(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_ovw_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))

    // 预置旧数据: 分区 20250101 / 20250102 / 20250103 各一行
    testUtils.executeSql(
      s"""INSERT INTO $parentTable VALUES
         |(1, 100, '20250101'),
         |(2, 200, '20250102'),
         |(3, 300, '20250103')""".stripMargin)

    // overwrite的数据只包含分区 20250101, 但声明了 20250101/20250102/20250103
    // 字面执行: 20250102/20250103 虽无数据也会被清空
    val data = Seq(
      Row(10L, 1000L, "20250101"),
      Row(11L, 1100L, "20250101")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .option(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS, "\"ds\"")
      .option(SourceProvider.WRITE_TARGET_PARTITION_VALUES,
        "\"20250101\"; \"20250102\"; \"20250103\"")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    // 声明的 3 个分区均被覆盖: 20250101 写入新数据(2行), 20250102/20250103 无数据被清空
    assert(readDf.count() == 2, s"expect 2 rows but got ${readDf.count()}")
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(10L, 1000L, "20250101"),
        Row(11L, 1100L, "20250101")
      )), schema
    )
    if (expected.except(readDf).count() > 0) {
      expected.show()
      readDf.show()
      throw new Exception("stage partition OVERWRITE declared: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * Fully automatic partition overwrite: no write.target_partition_columns/values configured.
   * Partition columns are auto-resolved from table metadata (logical_partition_columns),
   * partition values are auto-collected from the written data, only touched partitions are overwritten.
   */
  def stagePartitionOverwriteFullyAuto(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_fauto_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")

    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))

    // 预置旧数据: 分区 20250101 / 20250102 各一行
    testUtils.executeSql(
      s"""INSERT INTO $parentTable VALUES
         |(1, 100, '20250101'),
         |(2, 200, '20250102')""".stripMargin)

    // overwrite的数据只包含分区 20250101, 不配置任何分区参数
    val data = Seq(
      Row(10L, 1000L, "20250101"),
      Row(11L, 1100L, "20250101")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, "stage")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    // 分区20250101被覆盖为新数据(2行), 20250102无数据不受影响, 共 2+1=3 行
    assert(readDf.count() == 3, s"expect 3 rows but got ${readDf.count()}")
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(2L, 200L, "20250102"),
        Row(10L, 1000L, "20250101"),
        Row(11L, 1100L, "20250101")
      )), schema
    )
    if (expected.except(readDf).count() > 0) {
      expected.show()
      readDf.show()
      throw new Exception("stage partition fully-auto OVERWRITE: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  test("HoloTable.name() should not deadlock when called under a logging lock") {
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table)

    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .load()

    // SLF4J in this env binds to Log4j2, so we use a Log4j2 appender to simulate
    // Kyuubi's Log4j2DivertAppender which holds a WriteLock during append.
    val lock = new java.util.concurrent.locks.ReentrantReadWriteLock()
    val appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
      "deadlock-test-appender", null, null, true,
      org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
      override def append(event: org.apache.logging.log4j.core.LogEvent): Unit = {
        lock.writeLock().lock()
        try {} finally { lock.writeLock().unlock() }
      }
    }
    appender.start()

    val logCtx = org.apache.logging.log4j.LogManager.getContext(false)
      .asInstanceOf[org.apache.logging.log4j.core.LoggerContext]
    val rootLoggerConfig = logCtx.getConfiguration.getRootLogger
    rootLoggerConfig.addAppender(appender, null, null)
    logCtx.updateLoggers()
    try {
      // Close shared ExecutionPools so chooseBestMode must create fresh workers
      // whose startup log ("worker start") will go through our locking appender.
      val poolMapField = classOf[com.alibaba.hologres.client.impl.ExecutionPool]
        .getDeclaredField("POOL_MAP")
      poolMapField.setAccessible(true)
      val poolMap = poolMapField.get(null)
        .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String,
          com.alibaba.hologres.client.impl.ExecutionPool]]
      poolMap.values().forEach(_.close())
      poolMap.clear()

      // Hold the WriteLock (same type as appender) then call plan.toString() -> HoloTable.name().
      // WriteLock is reentrant for the same thread, so chooseBestMode()'s own logging
      // through the appender succeeds. But the worker thread (different thread) is blocked
      // from acquiring the WriteLock in the appender, reproducing the production deadlock.
      val future = java.util.concurrent.CompletableFuture.supplyAsync(
        new java.util.function.Supplier[String] {
          override def get(): String = {
            lock.writeLock().lock()
            try {
              // Spark 4.1 起 load() 的 unresolved plan 是 UnresolvedDataSource, 延迟到 analysis
              // 阶段才实例化 HoloTable, logical.toString() 不会调用 HoloTable.name(),
              // 测不到目标路径; 用 analyzed 强制解析, Spark 3.5 下同样有效
              readDf.queryExecution.analyzed.toString()
            } finally {
              lock.writeLock().unlock()
            }
          }
        }
      )

      try {
        val result = future.get(30, java.util.concurrent.TimeUnit.SECONDS)
        assert(result.contains("HoloTable"))
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          fail("HoloTable.name() deadlocked: name() is likely calling chooseBestMode() " +
            "again instead of returning cached tableType")
      }
    } finally {
      rootLoggerConfig.removeAppender("deadlock-test-appender")
      logCtx.updateLoggers()
      testUtils.dropTable(table)
    }
  }

  /**
   * COPY 模式(stream / bulk_load)写入逻辑分区表, 显式声明目标分区 (Hologres V5.0+).
   * COPY 为流式写入, SQL 必须在首批数据前确定, 因此必须显式声明分区, 无法自动采集;
   * 对 bulk_load 指定分区可将表锁降为分区锁. 不属于声明分区的数据会被服务端丢弃.
   */
  def copyPartitionInsert(writeMode: String): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_copy_${writeMode}_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    // bulk_load 要求无主键表, 此处两种模式统一用无主键逻辑分区表
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))

    // 数据跨 20250101/20250102 两个分区, 另含一行 20250103(未声明, 应被服务端丢弃)
    val data = Seq(
      Row(1L, 100L, "20250101"),
      Row(2L, 200L, "20250101"),
      Row(3L, 300L, "20250102"),
      Row(4L, 400L, "20250103")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema
    ).cache()

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .option(SourceProvider.WRITE_MODE, writeMode)
      .option(SourceProvider.WRITE_TARGET_PARTITION_COLUMNS, "\"ds\"")
      .option(SourceProvider.WRITE_TARGET_PARTITION_VALUES, "\"20250101\"; \"20250102\"")
      .mode(SaveMode.Append)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(schema)
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, parentTable)
      .load().orderBy("pk").cache()

    // 仅声明的 2 个分区共 3 行落库, ds=20250103 的行被丢弃
    assert(readDf.count() == 3,
      s"[$writeMode] expect 3 rows (ds=20250103 dropped) but got ${readDf.count()}")
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1L, 100L, "20250101"),
        Row(2L, 200L, "20250101"),
        Row(3L, 300L, "20250102")
      )), schema
    )
    if (expected.except(readDf).count() > 0) {
      expected.show()
      readDf.show()
      throw new Exception(s"[$writeMode] copy partition INSERT: data mismatch!")
    }

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * INT 分区列: 覆盖负数、零等边界值.
   * 写入与覆盖均用 stage 全自动模式, 验证数值型分区值 PARTITION (-100) 字面量与服务端匹配.
   */
  def stagePartitionIntColumn(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_int_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  pt int NOT NULL,
         |  PRIMARY KEY(pk, pt)
         |) LOGICAL PARTITION BY LIST(pt)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("pt", IntegerType)
    ))

    // 边界值: 负数 / 零 / 常规正数
    val data = Seq(
      Row(1L, 100L, -100),
      Row(2L, 200L, 0),
      Row(3L, 300L, 20250101)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).cache()
    writeStageFullyAuto(df, parentTable, SaveMode.Append)
    verifyTableData(df, parentTable, schema, "int partition INSERT")

    // 全自动覆盖: 只覆盖分区 pt=-100, 其他分区不受影响
    val ovwDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(10L, 1000L, -100),
      Row(11L, 1100L, -100)
    )), schema).cache()
    writeStageFullyAuto(ovwDf, parentTable, SaveMode.Overwrite)
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(2L, 200L, 0),
      Row(3L, 300L, 20250101),
      Row(10L, 1000L, -100),
      Row(11L, 1100L, -100)
    )), schema)
    verifyTableData(expected, parentTable, schema, "int partition OVERWRITE")

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * DATE 分区列: 覆盖 1970 年之前(epoch 前负 epochDay)、epoch 边界、常规日期.
   * 验证 LocalDate.ofEpochDay 对负值的分区值采集.
   */
  def stagePartitionDateColumn(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_date_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds date NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", DateType)
    ))

    // 边界值: 1970 之前 / epoch 当天 / 常规日期
    val data = Seq(
      Row(1L, 100L, Date.valueOf("1969-12-31")),
      Row(2L, 200L, Date.valueOf("1970-01-01")),
      Row(3L, 300L, Date.valueOf("2025-01-01"))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).cache()
    writeStageFullyAuto(df, parentTable, SaveMode.Append)
    verifyTableData(df, parentTable, schema, "date partition INSERT")

    // 全自动覆盖: 只覆盖 1970 之前的分区 1969-12-31
    val ovwDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(10L, 1000L, Date.valueOf("1969-12-31")),
      Row(11L, 1100L, Date.valueOf("1969-12-31"))
    )), schema).cache()
    writeStageFullyAuto(ovwDf, parentTable, SaveMode.Overwrite)
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(2L, 200L, Date.valueOf("1970-01-01")),
      Row(3L, 300L, Date.valueOf("2025-01-01")),
      Row(10L, 1000L, Date.valueOf("1969-12-31")),
      Row(11L, 1100L, Date.valueOf("1969-12-31"))
    )), schema)
    verifyTableData(expected, parentTable, schema, "date partition OVERWRITE")

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * TIMESTAMP 分区列: 覆盖 1970 之前 + 亚毫秒微秒精度双重边界.
   * 验证 collectPartitionValues 采集的分区值字面量保留微秒精度且负微秒值不偏移.
   * 注: 使用 timestamp(无时区)列, timestamptz 受 holo-client 既有 UTC wall-clock 时区问题限制, 不在此覆盖.
   */
  def stagePartitionTimestampColumn(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_ts_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ts timestamp NOT NULL,
         |  PRIMARY KEY(pk, ts)
         |) LOGICAL PARTITION BY LIST(ts)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ts", TimestampType)
    ))

    def tsWithMicros(str: String, micros: Int): Timestamp = {
      val ts = Timestamp.valueOf(str)
      ts.setNanos(micros * 1000)
      ts
    }

    // 边界值: 1970 之前且带亚毫秒微秒 / epoch 整点 / 常规时间带亚毫秒微秒
    val preEpochMicros = tsWithMicros("1969-12-31 23:59:59", 999999)
    val epoch = Timestamp.valueOf("1970-01-01 00:00:00")
    val modernMicros = tsWithMicros("2025-01-01 00:00:00", 1)
    val data = Seq(
      Row(1L, 100L, preEpochMicros),
      Row(2L, 200L, epoch),
      Row(3L, 300L, modernMicros)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).cache()
    writeStageFullyAuto(df, parentTable, SaveMode.Append)
    verifyTableData(df, parentTable, schema, "timestamp partition INSERT")

    // 全自动覆盖: 只覆盖 1970 之前带微秒的分区, 验证采集的分区字面量与服务端存储值精确匹配
    val ovwDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(10L, 1000L, preEpochMicros),
      Row(11L, 1100L, preEpochMicros)
    )), schema).cache()
    writeStageFullyAuto(ovwDf, parentTable, SaveMode.Overwrite)
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(2L, 200L, epoch),
      Row(3L, 300L, modernMicros),
      Row(10L, 1000L, preEpochMicros),
      Row(11L, 1100L, preEpochMicros)
    )), schema)
    verifyTableData(expected, parentTable, schema, "timestamp partition OVERWRITE")

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }

  /**
   * TEXT 分区列特殊字符: 单引号(验证 PARTITION 子句的 '' 转义)、中文与空格.
   */
  def stagePartitionTextSpecialChars(): Unit = {
    val suffix = randomSuffix
    val parentTable = s"table_for_holo_lp_text_sp_$suffix"

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    testUtils.executeSql(
      s"""CREATE TABLE $parentTable (
         |  pk bigint NOT NULL,
         |  id bigint,
         |  ds text NOT NULL,
         |  PRIMARY KEY(pk, ds)
         |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

    val schema = StructType(Array(
      StructField("pk", LongType),
      StructField("id", LongType),
      StructField("ds", StringType)
    ))

    val data = Seq(
      Row(1L, 100L, "it's"),
      Row(2L, 200L, "中文 分区")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).cache()
    writeStageFullyAuto(df, parentTable, SaveMode.Append)
    verifyTableData(df, parentTable, schema, "text special-char partition INSERT")

    // 全自动覆盖: 只覆盖含单引号的分区
    val ovwDf = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(10L, 1000L, "it's")
    )), schema).cache()
    writeStageFullyAuto(ovwDf, parentTable, SaveMode.Overwrite)
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(2L, 200L, "中文 分区"),
      Row(10L, 1000L, "it's")
    )), schema)
    verifyTableData(expected, parentTable, schema, "text special-char partition OVERWRITE")

    testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
  }
}
