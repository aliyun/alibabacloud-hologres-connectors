package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.SparkHoloTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SaveMode}

import java.io.InputStream
import java.sql.{Date, Timestamp}
import java.util.{Properties, TimeZone}

/** SparkHoloSinkSuite. */
abstract class SparkHoloSuiteBase extends QueryTest with SharedSparkSession {
  protected var testUtils: SparkHoloTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("setting.properties")
    val prop = new Properties()
    prop.load(inputStream)

    testUtils = new SparkHoloTestUtils()
    // Modify these parameters if don't skip the test.
    testUtils.username = prop.getProperty("USERNAME", System.getenv("HOLO_ACCESS_ID"))
    testUtils.password = prop.getProperty("PASSWORD", System.getenv("HOLO_ACCESS_KEY"))
    testUtils.jdbcUrl = prop.getProperty("JDBCURL", String.format("jdbc:postgresql://%s/%s", System.getenv("HOLO_ENDPOINT"), System.getenv("HOLO_TEST_DB")))
    testUtils.init()
  }

  override def afterAll(): Unit = {
    testUtils.client.close()
  }

  TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))

  val defaultSchema = StructType(Array(
    StructField("pk", LongType, nullable = false),
    StructField("st", ShortType),
    StructField("id", LongType),
    StructField("count", IntegerType),
    StructField("name", StringType),
    StructField("price", DecimalType(38, 12)),
    StructField("out_of_stock", BooleanType),
    StructField("weight", DoubleType),
    StructField("thick", FloatType),
    StructField("ts1", TimestampType),
    StructField("ts2", TimestampType),
    StructField("dt", DateType),
    StructField("by", BinaryType),
    StructField("inta", ArrayType(IntegerType)),
    StructField("longa", ArrayType(LongType)),
    StructField("floata", ArrayType(FloatType)),
    StructField("doublea", ArrayType(DoubleType)),
    StructField("boola", ArrayType(BooleanType)),
    StructField("stringa", ArrayType(StringType)),
    StructField("json_column", StringType),
    StructField("jsonb_column", StringType),
    StructField("rb_column", BinaryType)
  ))

  val defaultCreateHoloTableDDL = "create table TABLE_NAME (" +
    "    pk bigint primary key," +
    "    st smallint," +
    "    id bigint," +
    "    count int," +
    "    name text," +
    "    price numeric(38, 12)," +
    "    out_of_stock bool," +
    "    weight double precision," +
    "    thick float4," +
    "    ts1 timestamp," +
    "    ts2 timestamptz," +
    "    dt date," +
    "    by bytea," +
    "    inta int4[]," +
    "    longa int8[]," +
    "    floata float4[]," +
    "    doublea float8[]," +
    "    boola boolean[]," +
    "    stringa text[]," +
    "    json_column json," +
    "    jsonb_column jsonb," +
    "    rb_column roaringbitmap);"

  val defaultCreateHoloParentTableDDL =
    "create table PARENT_TABLE_NAME (" +
      "    pk bigint," +
      "    st smallint," +
      "    id bigint," +
      "    count int," +
      "    name text," +
      "    price numeric(38, 12)," +
      "    out_of_stock bool," +
      "    weight double precision," +
      "    thick float4," +
      "    ts1 timestamp," +
      "    ts2 timestamptz," +
      "    dt date," +
      "    by bytea," +
      "    inta int4[]," +
      "    longa int8[]," +
      "    floata float4[]," +
      "    doublea float8[]," +
      "    boola boolean[]," +
      "    stringa text[]," +
      "    json_column json," +
      "    jsonb_column jsonb," +
      "    rb_column roaringbitmap," +
      "    primary key(pk, dt)" +
      ") PARTITION BY LIST(dt);\n" +
      "CREATE TABLE TABLE_NAME PARTITION OF PARENT_TABLE_NAME FOR VALUES IN ('PARTITION_VALUE');"

  def randomSuffix: String = new java.util.Random().nextInt(1000000).toString

  // prepare data
  def prepareData(table: String): DataFrame = {
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
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2021-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(1L, 2.shortValue(), 6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("1971-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
      Row(2L, 3.shortValue(), 1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, 8.9F, Timestamp.valueOf("2020-02-29 16:12:33.123"),
        Timestamp.valueOf("2021-01-01 00:00:00.456"), Date.valueOf("2020-07-23"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray, json, jsonb, roaringBitmap),
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
      .option(SourceProvider.WRITE_MODE, "insertOrIgnore")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Append)
      .save()
    df
  }
}
