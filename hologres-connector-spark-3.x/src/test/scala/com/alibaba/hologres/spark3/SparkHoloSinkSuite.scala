package com.alibaba.hologres.spark3

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import com.alibaba.hologres.spark.SparkHoloTestUtils
import com.alibaba.hologres.spark3.sink.SourceProvider
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row, SaveMode}

/** SparkHoloSinkSuite. */
abstract class SparkHoloSinkSuite extends QueryTest with SharedSparkSession {
  protected var testUtils: SparkHoloTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new SparkHoloTestUtils()
    // Modify these parameters if don't skip the test.
    testUtils.database = "test_database"
    testUtils.username = "your_username"
    testUtils.password = "your_password"
    testUtils.endpoint = "Ip:Port"
    testUtils.jdbcUrl = "jdbc:postgresql://Ip:Port/test_database"
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

class SparkHoloWriteSuite extends SparkHoloSinkSuite {
  test("Simple write to Holo test.") {
    val table = Option(System.getenv("HOLO_TABLE_NAME")).getOrElse {
      testUtils.createTableSql(defaultSchema, "table_for_holo_test_1")
    }

    val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
    val intArray = Array(1, 2, 3)
    val longArray = Array(1L, 2L, 3L)
    val floatArray = Array(1.2F, 2.44F, 3.77F)
    val doubleArray = Array(1.222, 2.333, 3.444)
    val booleanArray = Array(true, false, false)
    val stringArray = Array("abcd", "bcde", "defg")

    val data = Seq(
      Row(-7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00"),
        Date.valueOf("2021-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
      Row(6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 12:00:00"),
        Date.valueOf("1971-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
      Row(1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, null, Timestamp.valueOf("2020-02-29 16:12:33"),
        Date.valueOf("2020-07-23"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      defaultSchema
    )

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.ENDPOINT, testUtils.endpoint)
      .option(SourceProvider.DATABASE, testUtils.database)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "insertOrIgnore")
      .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, df.schema.toDDL)
      .mode(SaveMode.Append)
      .save()

    // Read the data just written
    val readDf = spark.read
      .format("jdbc")
      .option("url", testUtils.jdbcUrl)
      .option("dbtable", table)
      .option("user", testUtils.username)
      .option("password", testUtils.password)
      .load()

    // Read the data just written and compare by column
    defaultSchema.foreach(coulmn => {
      val diffDf = df.select(coulmn.name).exceptAll(readDf.select(coulmn.name))
      if (!diffDf.isEmpty) {
        diffDf.show()
        throw new Exception("The data read is inconsistent with the data written！！！")
      }
    })
  }

  test("Simple update to Holo test.") {
    val byteA = Array(4.toByte, 5.toByte, 6.toByte, 'q'.toByte, 'e'.toByte)
    val intA = Array(4, 5, 6)
    val doubleA = Array(2.333, 3.444, 4.555)

    val data = Seq(
      Row(-7L, 20, "phone1", 6.7F, Timestamp.valueOf("2021-03-29 00:00:00"), byteA, intA, doubleA),
      Row(6L, -30, "phone2", 7.8F, Timestamp.valueOf("2021-04-01 12:00:00"), byteA, intA, doubleA)
    )

    val newSchema = StructType(Array(
      StructField("id", LongType),
      StructField("count", IntegerType),
      StructField("name", StringType, false), //false表示此Field不允许为null
      StructField("thick", FloatType),
      StructField("time", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType))
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      newSchema
    )

    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.ENDPOINT, testUtils.endpoint)
      .option(SourceProvider.DATABASE, testUtils.database)
      .option(SourceProvider.TABLE, "table_for_holo_test_1")
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, df.schema.toDDL)
      .mode(SaveMode.Append)
      .save()
  }

  test("Spark sql write to Holo test.") {
    // Read from some table, for example: table_for_holo_test_1
    // This example is read from postgres, which can be any spark support data source
    val readDf = spark.read
      .format("jdbc")
      .option("url", testUtils.jdbcUrl)
      .option("dbtable", "table_for_holo_test_1")
      .option("user", testUtils.username)
      .option("password", testUtils.password)
      .load()

    val table = Option(System.getenv("HOLO_TABLE_NAME")).getOrElse {
      testUtils.createTableSql(readDf.schema, "table_for_holo_test_2")
    }

    // Write to hologres table, for example: table_for_holo_test_2
    readDf.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.ENDPOINT, testUtils.endpoint)
      .option(SourceProvider.DATABASE, testUtils.database)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, readDf.schema.toDDL)
      .mode(SaveMode.Append)
      .save()
  }
}
