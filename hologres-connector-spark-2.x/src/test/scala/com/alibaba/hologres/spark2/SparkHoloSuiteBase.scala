package com.alibaba.hologres.spark2

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
    testUtils.username = prop.getProperty("USERNAME")
    testUtils.password = prop.getProperty("PASSWORD")
    testUtils.jdbcUrl = prop.getProperty("JDBCURL")
    testUtils.init()
  }

  override def afterAll(): Unit = {
    testUtils.client.close()
  }

  TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))

  val defaultSchema = StructType(Array(
    StructField("id", LongType),
    StructField("count", IntegerType),
    StructField("name", StringType, false), //false表示此Field不允许为null
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
    StructField("stringa", ArrayType(StringType))
  ))
}
