package com.alibaba.hologres.spark2

import com.alibaba.hologres.spark.WriteType
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

class SparkHoloReadWriteSuite extends SparkHoloSuiteBase {

  def dataTypeTest(writeType: WriteType.Value, querySource: Boolean = false): Unit = {
    val table = "table_for_holo_test_1"
    testUtils.dropTable(table)
    testUtils.createTable(defaultCreateHoloTableDDL, table, writeType != WriteType.BULK_LOAD)

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
      .option(SourceProvider.COPY_WRITE_MODE, writeType.toString)
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .save()

    val sourceKey: String = if (querySource) SourceProvider.QUERY else SourceProvider.TABLE
    val sourceValue: String = if (querySource) "select * from " + table else table
    // Read the data just written
    val readDf = spark.read
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(sourceKey, sourceValue)
      .option(SourceProvider.MAX_PARTITION_COUNT, 4)
      .load().orderBy("pk").cache()

    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
  }

  test("data type test insert.") {
    dataTypeTest(WriteType.DISABLE)
  }

  test("data type test copy.") {
    dataTypeTest(WriteType.STREAM)
  }

  test("data type test bulk load.") {
    dataTypeTest(WriteType.BULK_LOAD)
  }

  test("data type test copy, and use a query source.") {
    dataTypeTest(WriteType.STREAM, querySource = true)
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

    val copyMode = if (useCopy) "stream" else "disable"
    df.write
      .format("hologres")
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, copyMode)
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.MAX_PARTITION_COUNT, 4)
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


  test("SaveMode = overwrite.") {
    val table = "table_for_holo_test_2"
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
      StructField("name", StringType),
      StructField("thick", FloatType),
      StructField("time", TimestampType),
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "bulk_load")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "bulk_load")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.MAX_PARTITION_COUNT, 4)
      .load().orderBy("pk").cache()

    assert(df.count() == 4)
    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
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
      StructField("name", StringType),
      StructField("thick", FloatType),
      StructField("time", TimestampType),
      StructField("by", BinaryType),
      StructField("inta", ArrayType(IntegerType)),
      StructField("doublea", ArrayType(DoubleType)),
      StructField("dt", DateType)
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "stream")
      .option(SourceProvider.RESHUFFLE_BY_HOLO_DISTRIBUTION_KEY, "true")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "bulk_load")
      .option(SourceProvider.RESHUFFLE_BY_HOLO_DISTRIBUTION_KEY, "true")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val readDf = spark.read
      .format("hologres")
      .schema(newSchema) // 指定读取哪些字段
      .option(SourceProvider.USERNAME, testUtils.username)
      .option(SourceProvider.PASSWORD, testUtils.password)
      .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
      .option(SourceProvider.TABLE, table)
      .option(SourceProvider.MAX_PARTITION_COUNT, 4)
      .load().orderBy("pk").cache()

    assert(df.count() == 4)
    // compare read and write
    if (df.except(readDf).count() > 0) {
      df.show()
      readDf.show()
      throw new Exception("The data read is inconsistent with the data written！！！")
    }
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
      StructField("name", StringType),
      StructField("thick", FloatType),
      StructField("time", TimestampType),
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "stream")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
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
      .option(SourceProvider.WRITE_MODE, "insertOrUpdate")
      .option(SourceProvider.COPY_WRITE_MODE, "stream")
      .option(SourceProvider.COPY_WRITE_DIRTY_DATA_CHECK, "true")
      .mode(SaveMode.Overwrite)
      .save()

    var joinDf = df1.join(df2, "pk").select(
      df1("pk").alias("pk"),
      df1("id").alias("id"),
      df2("count").alias("count"),
      df1("name").alias("name"),
      df2("thick").alias("thick"),
      df1("time").alias("time"),
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
      .option(SourceProvider.QUERY, String.format("select t1.pk, t1.id, t2.count, t1.name, t2.thick, t1.time, " +
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
      df1("name").alias("name"),
      df2("thick").alias("thick"),
      df1("time").alias("time")
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
      .option(SourceProvider.QUERY, String.format("select t1.pk, t1.id, t2.count, t1.name, t2.thick, t1.time, " +
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
        .option(SourceProvider.MAX_PARTITION_COUNT, 4)
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
        .option(SourceProvider.MAX_PARTITION_COUNT, 4)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("column id in hologres table table_for_holo_test_1 type does not match: spark type: StringType, hologres type: int8")) {
          throw new RuntimeException(e)
        }
    }
  }


  test("read params illegal.") {
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
      val readDf = spark.read
        .format("hologres")
        .schema(newSchema)
        .option(SourceProvider.USERNAME, testUtils.username)
        .option(SourceProvider.PASSWORD, testUtils.password)
        .option(SourceProvider.JDBCURL, testUtils.jdbcUrl)
        .option(SourceProvider.TABLE, table)
        .option(SourceProvider.MAX_PARTITION_COUNT, 4)
        .option(SourceProvider.QUERY, "select * from " + table)
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
        .option(SourceProvider.MAX_PARTITION_COUNT, 4)
        .load().filter("pk = 0 or pk = 1").orderBy("pk").cache()
    } catch {
      case e: Exception =>
        if (!e.getMessage.contains("Missing necessary parameter 'table'. If table is not provided, please provide parameter 'query' for read")) {
          throw new RuntimeException(e)
        }
    }
  }

}
