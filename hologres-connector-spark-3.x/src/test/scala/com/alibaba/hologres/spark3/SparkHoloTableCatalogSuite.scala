package com.alibaba.hologres.spark3

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkHoloTableCatalogSuite extends SparkHoloSuiteBase {

  test("Holo Table Catalog Test") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark3.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_external.read.max_task_count", 20)
    val defaultNamespace = "public"
    // a hack to skip jsonb type test
    val ddl = defaultCreateHoloTableDDL.replace("jsonb_column jsonb", "jsonb_column json")
    val table1 = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table1)
    testUtils.createTable(ddl, table1)
    val table2 = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table2)
    testUtils.createTable(ddl, table2)
    val expectDf = prepareData(table1)

    spark.sql("use hologres_external")
    val namespaces = spark.sql("show namespaces;")
    if (!namespaces.select("namespace").collect().exists(row => row.getString(0) == defaultNamespace)) {
      throw new Exception("namespace not found: " + defaultNamespace)
    }

    val tables = spark.sql("show tables;").select("tableName").collect()
    spark.sql("show tables;").select("tableName").show()
    if (!tables.sameElements(spark.sql(s"show tables in $defaultNamespace;").select("tableName").collect())) {
      throw new Exception("should not happen!")
    }
    var count = 0
    for (table <- tables) {
      if (table.getString(0) == s"$table1" || table.getString(0) == s"$table2") {
        count = count + 1
      }
    }
    if (count != 2) {
      throw new Exception(s"$table1 or $table2 not found!")
    }

    val res1 = spark.sql(s"select * from $table1;").orderBy("id").cache()
    val res2 = spark.sql(s"select * from $defaultNamespace.$table1;").orderBy("id").cache()
    spark.sql(s"insert into $table2 select * from $table1;")
    val res3 = spark.sql(s"select * from $defaultNamespace.$table2;").orderBy("id").cache()

    if (!res1.collect().sameElements(res2.collect())
      || !res1.collect().sameElements(res3.collect())
      || !expectDf.orderBy("id").collect().sameElements(res2.collect())) {
      res1.show()
      res2.show()
      res3.show()
      expectDf.show()
      throw new Exception("result not same!")
    }
    testUtils.dropTable(table1)
    testUtils.dropTable(table2)
  }

  test("Holo Table Catalog Negative Test") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark3.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_external.read.max_task_count", 20)
    spark.conf.set("spark.sql.catalog.hologres_external.read.mode", "bulk_read")
    val ddl = "create table TABLE_NAME (" +
      "    pk bigint primary key," +
      "    dc1 decimal(38,6)," +
      "    dc2 decimal(24,4)) with (table_group='tg_1');"
    val table = "table_for_holo_test_" + randomSuffix
    testUtils.createTable(ddl, table)

    spark.sql("use hologres_external")

    // fields count not match
    try {
      spark.sql(s"insert into $table select 1, cast(100.123456 as decimal(38,6));")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("schema length not match"))
    }
    // fields type not match
    try {
      spark.sql(s"insert into $table select cast(1 as long), cast(100.123456 as decimal(38,6)), cast(100.123456 as decimal(38,6));")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("schema not match in field 2, \nspark schema type: DecimalType(24,4), \nwrite schema type: DecimalType(38,6)"))
    }

    spark.sql(s"insert into $table select cast(1 as int), cast(100.123456 as decimal(38,6)), cast(100.123456 as decimal(24,4));")
    val df = spark.sql(s"select * from $table;").cache()
    val data = Seq(
      Row(1L, BigDecimal(100.123456), BigDecimal(100.1235))
    )

    if (!df.rdd.collect().head.toString().equals(data.head.toString())) {
      System.out.println(df.rdd.collect().head.toString())
      System.out.println(data.head.toString())
      throw new Exception("result not same!")
    }

    testUtils.dropTable(table)
  }

  test("Holo Table Catalog write table with generated column") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark3.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_external.read.max_task_count", 20)
    spark.conf.set("spark.sql.catalog.hologres_external.write.mode", "stage")

    val table = "table_for_holo_test_" + randomSuffix
    testUtils.dropTable(table)
    // 建表包含生成列 v_time_bucket
    val ddl = "create table TABLE_NAME (" +
      "    pk bigint primary key," +
      "    v_time bigint not null," +
      "    name text," +
      "    v_time_bucket bigint GENERATED ALWAYS AS (v_time / 600000) STORED NOT NULL);"
    testUtils.createTable(ddl, table)

    spark.sql("use hologres_external")

    // 通过catalog写入，源数据不包含生成列（模拟用户从源表SELECT写入目标表的场景）
    spark.sql("CREATE OR REPLACE TEMP VIEW gen_col_source AS " +
      "SELECT cast(1 as bigint) as pk, cast(1200000 as bigint) as v_time, 'aaa' as name " +
      "UNION ALL SELECT cast(2 as bigint), cast(1800000 as bigint), 'bbb' " +
      "UNION ALL SELECT cast(3 as bigint), cast(600000 as bigint), 'ccc'")
    spark.sql(s"insert into $table select * from gen_col_source;")

    // 读取验证生成列计算正确
    val df = spark.sql(s"select * from $table order by pk;").cache()
    val result = df.collect()
    assert(result.length == 3)
    // v_time_bucket = v_time / 600000
    assert(result(0).getAs[Long]("v_time_bucket") == 2L) // 1200000 / 600000 = 2
    assert(result(1).getAs[Long]("v_time_bucket") == 3L) // 1800000 / 600000 = 3
    assert(result(2).getAs[Long]("v_time_bucket") == 1L) // 600000 / 600000 = 1

    testUtils.dropTable(table)
  }

}
