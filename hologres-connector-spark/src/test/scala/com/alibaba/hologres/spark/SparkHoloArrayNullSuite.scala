package com.alibaba.hologres.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkHoloArrayNullSuite extends SparkHoloSuiteBase {

  private val WriteModes = Seq("insert", "stream", "bulk_load", "bulk_load_on_conflict", "stage")
  private val ReadModes = Seq("select", "bulk_read", "bulk_read_compressed")

  private def configureCatalog(writeMode: String = "insert", readMode: String = "select"): Unit = {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_external.read.max_task_count", 20)
    spark.conf.set("spark.sql.catalog.hologres_external.read.mode", readMode)
    spark.conf.set("spark.sql.catalog.hologres_external.write.mode", writeMode)
    spark.sql("use hologres_external")
  }

  private def createArrayTable(table: String): Unit = {
    testUtils.dropTable(table)
    testUtils.createTable(
      "create table TABLE_NAME (" +
        "pk bigint primary key," +
        "longa int8[]," +
        "inta int4[]," +
        "boola boolean[]," +
        "floata float4[]," +
        "doublea float8[]," +
        "stringa text[]);",
      table)
  }

  private def insertArraysWithNullElements(table: String): Unit = {
    spark.sql(
      s"insert into $table select " +
        "cast(1 as bigint), " +
        "array(cast(10 as bigint), cast(null as bigint), cast(30 as bigint)), " +
        "array(cast(1 as int), cast(null as int), cast(3 as int)), " +
        "array(true, cast(null as boolean), false), " +
        "array(cast(1.5 as float), cast(null as float), cast(3.5 as float)), " +
        "array(cast(2.5 as double), cast(null as double), cast(4.5 as double)), " +
        "array('a', cast(null as string), 'c')")
  }

  private def insertArraysFromNonNullableSchemaView(table: String): Unit = {
    val nonNullableArraySchema = StructType(Seq(
      StructField("pk", LongType, nullable = false),
      StructField("longa", ArrayType(LongType, containsNull = false), nullable = true),
      StructField("inta", ArrayType(org.apache.spark.sql.types.IntegerType, containsNull = false), nullable = true),
      StructField("boola", ArrayType(org.apache.spark.sql.types.BooleanType, containsNull = false), nullable = true),
      StructField("floata", ArrayType(org.apache.spark.sql.types.FloatType, containsNull = false), nullable = true),
      StructField("doublea", ArrayType(org.apache.spark.sql.types.DoubleType, containsNull = false), nullable = true),
      StructField("stringa", ArrayType(StringType, containsNull = false), nullable = true)
    ))
    val data = Seq(Row(
      2L,
      Seq[java.lang.Long](10L, 20L, 30L),
      Seq[java.lang.Integer](1, 2, 3),
      Seq[java.lang.Boolean](java.lang.Boolean.TRUE, java.lang.Boolean.TRUE, java.lang.Boolean.FALSE),
      Seq[java.lang.Float](1.5F, 2.5F, 3.5F),
      Seq[java.lang.Double](2.5D, 3.5D, 4.5D),
      Seq[String]("a", "b", "c")
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), nonNullableArraySchema)
    df.createOrReplaceTempView("tmp_non_nullable_array_source")
    spark.sql(s"insert into $table select * from tmp_non_nullable_array_source")
    spark.catalog.dropTempView("tmp_non_nullable_array_source")
  }

  // Scala 2.13 中 Seq 是 immutable.Seq 的别名, 而 Row.getAs 返回 mutable.ArraySeq,
  // 直接 getAs[Seq[_]] 会 ClassCastException; 显式用 scala.collection.Seq 再 toSeq, 2.12 同样成立
  private def assertArrayElementsReadAsDefaults(table: String): Unit = {
    val row = spark.sql(s"select * from $table").collect().head
    assert(row.getAs[scala.collection.Seq[java.lang.Long]]("longa").toSeq == Seq(10L, 0L, 30L))
    assert(row.getAs[scala.collection.Seq[java.lang.Integer]]("inta").toSeq == Seq(1, 0, 3))
    assert(row.getAs[scala.collection.Seq[java.lang.Boolean]]("boola").toSeq == Seq(true, false, false))
    assert(row.getAs[scala.collection.Seq[java.lang.Float]]("floata").toSeq == Seq(1.5F, 0.0F, 3.5F))
    assert(row.getAs[scala.collection.Seq[java.lang.Double]]("doublea").toSeq == Seq(2.5D, 0.0D, 4.5D))
    assert(row.getAs[scala.collection.Seq[String]]("stringa").toSeq == Seq("a", "", "c"))
  }

  WriteModes.foreach { writeMode =>
    test(s"Holo Table Catalog write array with null elements in $writeMode mode") {
      val table = "table_for_holo_array_null_test_" + writeMode + "_" + randomSuffix
      createArrayTable(table)
      try {
        configureCatalog(writeMode = writeMode)
        insertArraysWithNullElements(table)
        assertArrayElementsReadAsDefaults(table)
      } finally {
        testUtils.dropTable(table)
      }
    }
  }

  test("Holo Table Catalog accepts non-nullable array source schema") {
    val table = "table_for_holo_array_null_non_nullable_source_" + randomSuffix
    createArrayTable(table)
    try {
      configureCatalog(writeMode = "insert")
      insertArraysFromNonNullableSchemaView(table)
      val row = spark.sql(s"select * from $table").collect().head
      assert(row.getAs[scala.collection.Seq[java.lang.Long]]("longa").toSeq == Seq(10L, 20L, 30L))
      assert(row.getAs[scala.collection.Seq[java.lang.Integer]]("inta").toSeq == Seq(1, 2, 3))
      assert(row.getAs[scala.collection.Seq[java.lang.Boolean]]("boola").toSeq == Seq(true, true, false))
      assert(row.getAs[scala.collection.Seq[java.lang.Float]]("floata").toSeq == Seq(1.5F, 2.5F, 3.5F))
      assert(row.getAs[scala.collection.Seq[java.lang.Double]]("doublea").toSeq == Seq(2.5D, 3.5D, 4.5D))
      assert(row.getAs[scala.collection.Seq[String]]("stringa").toSeq == Seq("a", "b", "c"))
    } finally {
      testUtils.dropTable(table)
    }
  }

  ReadModes.foreach { readMode =>
    test(s"Holo Table Catalog read array elements as defaults in $readMode mode") {
      val table = "table_for_holo_array_null_read_test_" + readMode + "_" + randomSuffix
      createArrayTable(table)
      try {
        configureCatalog(writeMode = "insert", readMode = "select")
        insertArraysWithNullElements(table)

        configureCatalog(readMode = readMode)
        assertArrayElementsReadAsDefaults(table)
      } finally {
        testUtils.dropTable(table)
      }
    }
  }
}
