package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.SparkHoloTestUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

import java.io.InputStream
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
    StructField("time", TimestampType),
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
    "    time timestamptz," +
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
    "    time timestamptz," +
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
}
