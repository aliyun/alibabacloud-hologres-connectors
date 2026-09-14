package com.alibaba.hologres.spark

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkHoloTableCatalogSuite extends SparkHoloSuiteBase {

  test("Holo Table Catalog Test") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark.HoloTableCatalog")
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

    val expectedTables = Set(table1, table2)
    val defaultTables = spark.sql("show tables;").select("tableName").collect()
      .map(_.getString(0)).toSet
    val explicitTables = spark.sql(s"show tables in $defaultNamespace;").select("tableName").collect()
      .map(_.getString(0)).toSet
    assert(expectedTables.subsetOf(defaultTables),
      s"$expectedTables not found in default namespace tables: $defaultTables")
    assert(expectedTables.subsetOf(explicitTables),
      s"$expectedTables not found in $defaultNamespace tables: $explicitTables")

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
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark.HoloTableCatalog")
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

  test("Holo Table Catalog writes partial columns to partition parent table by name") {
    val catalog = "hologres_partial_external"
    spark.conf.set(s"spark.sql.catalog.$catalog", "com.alibaba.hologres.spark.HoloTableCatalog")
    spark.conf.set(s"spark.sql.catalog.$catalog.username", testUtils.username)
    spark.conf.set(s"spark.sql.catalog.$catalog.password", testUtils.password)
    spark.conf.set(s"spark.sql.catalog.$catalog.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set(s"spark.sql.catalog.$catalog.write.mode", "insert")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.insert.dynamic_partition", "true")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.on_conflict_action", "INSERT_OR_UPDATE")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.catalog.by_name", "true")

    val parentTable = "orders_partitioned_" + randomSuffix
    val childTable = parentTable + "_shanghai"
    try {
      testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
      testUtils.executeSql(
        s"""CREATE TABLE $parentTable (
           |  order_id BIGINT,
           |  customer_id INT,
           |  product_name TEXT,
           |  region TEXT,
           |  amount INT,
           |  order_date TEXT,
           |  PRIMARY KEY (order_id, region)
           |) PARTITION BY LIST (region)""".stripMargin)
      testUtils.executeSql(
        s"CREATE TABLE $childTable PARTITION OF $parentTable FOR VALUES IN ('shanghai')")

      spark.sql(
        s"""INSERT INTO $catalog.`public`.$parentTable
           |SELECT CAST(1 AS BIGINT) AS order_id,
           |       11 AS customer_id,
           |       'iPhone' AS product_name,
           |       'shanghai' AS region,
           |       111 AS amount,
           |       'a' AS order_date""".stripMargin)

      // 输入列顺序与目标表不同，且不传 order_date；按名称更新时 order_date 必须保留。
      spark.sql(
        s"""INSERT INTO $catalog.`public`.$parentTable
           |SELECT 222 AS amount,
           |       'shanghai' AS region,
           |       CAST(1 AS BIGINT) AS order_id,
           |       'iPhone1' AS product_name,
           |       222 AS customer_id""".stripMargin)

      val updated = spark.sql(
        s"SELECT * FROM $catalog.`public`.$parentTable WHERE order_id = 1").head()
      assert(updated.getAs[Int]("customer_id") == 222)
      assert(updated.getAs[String]("product_name") == "iPhone1")
      assert(updated.getAs[Int]("amount") == 222)
      assert(updated.getAs[String]("order_date") == "a")

      // 新主键只写部分列时，未传入的可空列由 Hologres 保持为 NULL。
      spark.sql(
        s"""INSERT INTO $catalog.`public`.$parentTable
           |SELECT CAST(2 AS BIGINT) AS order_id,
           |       'shanghai' AS region,
           |       333 AS amount""".stripMargin)
      val inserted = spark.sql(
        s"SELECT * FROM $catalog.`public`.$parentTable WHERE order_id = 2").head()
      assert(inserted.getAs[Int]("amount") == 333)
      assert(inserted.isNullAt(inserted.fieldIndex("order_date")))

      // 显式传入 NULL 与不传字段语义不同：该字段会被更新为 NULL。
      spark.sql(
        s"""INSERT INTO $catalog.`public`.$parentTable
           |SELECT CAST(1 AS BIGINT) AS order_id,
           |       'shanghai' AS region,
           |       CAST(NULL AS STRING) AS order_date""".stripMargin)
      val explicitNull = spark.sql(
        s"SELECT order_date FROM $catalog.`public`.$parentTable WHERE order_id = 1").head()
      assert(explicitNull.isNullAt(0))
    } finally {
      testUtils.executeSql(s"DROP TABLE IF EXISTS $parentTable CASCADE")
    }
  }

  test("Holo Table Catalog supports Spark-side filtered conditional upsert") {
    val catalog = "hologres_conditional_external"
    spark.conf.set(s"spark.sql.catalog.$catalog", "com.alibaba.hologres.spark.HoloTableCatalog")
    spark.conf.set(s"spark.sql.catalog.$catalog.username", testUtils.username)
    spark.conf.set(s"spark.sql.catalog.$catalog.password", testUtils.password)
    spark.conf.set(s"spark.sql.catalog.$catalog.jdbcurl", testUtils.jdbcUrl)
    // 关闭 filter 下推: 该 case 通过 catalog 读目标表时, WHERE 子句下推 + bulk_read 触发的 COPY OUT
    // 在共享实例 holohub-catalog 上偶发触发 "Failed to get available shards" 瞬时 dispatch 错误;
    // 关闭下推后 read 走全表 + Spark 侧过滤, 避开此实例侧问题。
    spark.conf.set(s"spark.sql.catalog.$catalog.read.push_down_predicate", "false")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.mode", "insert")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.on_conflict_action", "INSERT_OR_UPDATE")
    spark.conf.set(s"spark.sql.catalog.$catalog.write.catalog.by_name", "true")

    val table = "conditional_upsert_" + randomSuffix
    val newData = "new_data_" + randomSuffix
    val existingData = "existing_data_" + randomSuffix
    val filteredData = "filtered_data_" + randomSuffix
    try {
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table")
      testUtils.executeSql(
        s"""CREATE TABLE $table (
           |  id BIGINT,
           |  create_date TEXT,
           |  ts_ms BIGINT,
           |  PRIMARY KEY (id, create_date)
           |)""".stripMargin)
      testUtils.executeSql(
        s"INSERT INTO $table VALUES (1001, '20260307', 100), (1002, '20260307', 200), (1003, '20260307', 300)")

      spark.sql(
        s"""CREATE OR REPLACE TEMPORARY VIEW $newData AS
           |SELECT CAST(1001 AS BIGINT) AS id, '20260307' AS create_date, CAST(50 AS BIGINT) AS ts_ms
           |UNION ALL SELECT CAST(1002 AS BIGINT), '20260307', CAST(999 AS BIGINT)
           |UNION ALL SELECT CAST(1003 AS BIGINT), '20260307', CAST(300 AS BIGINT)
           |UNION ALL SELECT CAST(1004 AS BIGINT), '20260307', CAST(400 AS BIGINT)""".stripMargin)
      spark.sql(
        s"""CREATE OR REPLACE TEMPORARY VIEW $existingData AS
           |SELECT id, create_date, ts_ms AS old_ts_ms
           |FROM $catalog.`public`.$table
           |WHERE create_date = '20260307'""".stripMargin)
      spark.sql(
        s"""CREATE OR REPLACE TEMPORARY VIEW $filteredData AS
           |SELECT n.id, n.create_date, n.ts_ms
           |FROM $newData n
           |LEFT JOIN $existingData e
           |  ON n.id = e.id AND n.create_date = e.create_date
           |WHERE e.old_ts_ms IS NULL
           |   OR COALESCE(n.ts_ms, 0) >= COALESCE(e.old_ts_ms, 0)""".stripMargin)
      spark.sql(
        s"""INSERT INTO $catalog.`public`.$table
           |SELECT id, create_date, ts_ms FROM $filteredData""".stripMargin)

      val result = spark.sql(
        s"SELECT id, ts_ms FROM $catalog.`public`.$table ORDER BY id").collect()
      assert(result.map(row => row.getLong(0) -> row.getLong(1)).toSeq ==
        Seq(1001L -> 100L, 1002L -> 999L, 1003L -> 300L, 1004L -> 400L))
    } finally {
      spark.catalog.dropTempView(newData)
      spark.catalog.dropTempView(existingData)
      spark.catalog.dropTempView(filteredData)
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table")
    }
  }

  test("Holo Table Catalog write table with generated column") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark.HoloTableCatalog")
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

  /**
   * 通过 Hologres Catalog 读取 VIEW, 验证 sourceType 被正确设置为 "VIEW" 而非默认 "TABLE".
   * 回归场景: 之前 getHoloSchema() 返回的 (TableSchema, sourceType) 中 sourceType 被丢弃,
   * 导致 VIEW 被错误当作物理表处理, 下游 chooseBestMode 可能走入不支持的分支.
   */
  test("Holo Table Catalog read from VIEW") {
    spark.conf.set("spark.sql.catalog.hologres_view_ext", "com.alibaba.hologres.spark.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_view_ext.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_view_ext.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_view_ext.jdbcurl", testUtils.jdbcUrl)

    val table = "table_for_view_test_" + randomSuffix
    val view = "view_for_catalog_test_" + randomSuffix
    try {
      // 1. 建表并写入数据
      testUtils.executeSql(s"DROP VIEW IF EXISTS $view")
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table")
      testUtils.executeSql(
        s"""CREATE TABLE $table (
           |  pk bigint NOT NULL,
           |  name text,
           |  value bigint,
           |  PRIMARY KEY(pk)
           |)""".stripMargin)
      testUtils.executeSql(
        s"INSERT INTO $table VALUES (1, 'alpha', 100), (2, 'beta', 200), (3, 'gamma', 300)")

      // 2. 创建 VIEW
      testUtils.executeSql(
        s"CREATE VIEW $view AS SELECT pk, name, value FROM $table WHERE value > 0")

      // 3. 通过 Catalog 读取 VIEW
      spark.sql("use hologres_view_ext")
      val result = spark.sql(s"select * from $view order by pk;").collect()

      // 4. 验证数据正确性
      assert(result.length == 3, s"Expected 3 rows from view, got ${result.length}")
      assert(result(0).getAs[Long]("pk") == 1L)
      assert(result(0).getAs[String]("name") == "alpha")
      assert(result(0).getAs[Long]("value") == 100L)
      assert(result(1).getAs[Long]("pk") == 2L)
      assert(result(1).getAs[String]("name") == "beta")
      assert(result(2).getAs[Long]("pk") == 3L)
    } finally {
      testUtils.executeSql(s"DROP VIEW IF EXISTS $view")
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table")
    }
  }

  /**
   * 通过 Hologres Catalog 写入逻辑分区表, 覆盖三条 SQL/API 路径:
   * 1. `INSERT INTO t PARTITION (ds='x')` 静态分区插入;
   * 2. `INSERT OVERWRITE t PARTITION (ds='x')` 静态分区覆盖(spec 翻译为 EqualNullSafe filters);
   * 3. `partitionOverwriteMode=dynamic` 的 `INSERT OVERWRITE` 与 `writeTo().overwritePartitions()`
   *    动态分区覆盖(overwriteDynamicPartitions), 仅覆盖数据中实际出现的分区.
   */
  test("Holo Table Catalog logical partition write and overwrite") {
    spark.conf.set("spark.sql.catalog.hologres_lp_external", "com.alibaba.hologres.spark.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_lp_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_lp_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_lp_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_lp_external.write.mode", "stage")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val table = "table_for_holo_lp_catalog_" + randomSuffix
    try {
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table CASCADE")
      testUtils.executeSql(
        s"""CREATE TABLE $table (
           |  pk bigint NOT NULL,
           |  id bigint,
           |  ds text NOT NULL,
           |  PRIMARY KEY(pk, ds)
           |) LOGICAL PARTITION BY LIST(ds)""".stripMargin)

      spark.sql("use hologres_lp_external")

      def countOf(ds: String): Long = {
        spark.sql(s"SELECT count(*) AS cnt FROM $table WHERE ds = '$ds'").collect()(0).getLong(0)
      }

      // 预置数据: 分区 20250101 / 20250102 各一行
      spark.sql("CREATE OR REPLACE TEMP VIEW lp_init AS " +
        "SELECT cast(1 as bigint) AS pk, cast(100 as bigint) AS id, '20250101' AS ds " +
        "UNION ALL SELECT cast(2 as bigint), cast(200 as bigint), '20250102'")
      spark.sql(s"INSERT INTO $table SELECT * FROM lp_init")
      assert(countOf("20250101") == 1 && countOf("20250102") == 1)

      // 1. 静态 PARTITION 子句插入新分区, SELECT 不包含分区列
      spark.sql("CREATE OR REPLACE TEMP VIEW lp_p3 AS " +
        "SELECT cast(3 as bigint) AS pk, cast(300 as bigint) AS id")
      spark.sql(s"INSERT INTO $table PARTITION (ds = '20250103') SELECT * FROM lp_p3")
      assert(countOf("20250103") == 1)

      // 2. 静态 PARTITION 子句覆盖: 仅清空并重写 20250101, 其他分区不受影响
      spark.sql("CREATE OR REPLACE TEMP VIEW lp_ovw AS " +
        "SELECT cast(10 as bigint) AS pk, cast(1000 as bigint) AS id " +
        "UNION ALL SELECT cast(11 as bigint), cast(1100 as bigint)")
      spark.sql(s"INSERT OVERWRITE $table PARTITION (ds = '20250101') SELECT * FROM lp_ovw")
      assert(countOf("20250101") == 2)
      assert(countOf("20250102") == 1 && countOf("20250103") == 1)

      // 3a. partitionOverwriteMode=dynamic 的 INSERT OVERWRITE: 数据只含 20250102,
      //     仅覆盖该分区, 20250101/20250103 不受影响
      spark.sql("CREATE OR REPLACE TEMP VIEW lp_dyn AS " +
        "SELECT cast(20 as bigint) AS pk, cast(2000 as bigint) AS id, '20250102' AS ds")
      spark.sql(s"INSERT OVERWRITE $table SELECT * FROM lp_dyn")
      assert(countOf("20250102") == 1)
      assert(countOf("20250101") == 2 && countOf("20250103") == 1)

      // 3b. DataFrame API writeTo().overwritePartitions(): 同样只覆盖数据中出现的分区
      val df = spark.sql("SELECT cast(30 as bigint) AS pk, cast(3000 as bigint) AS id, '20250103' AS ds")
      df.writeTo(s"hologres_lp_external.$table").overwritePartitions()
      assert(countOf("20250103") == 1)
      assert(countOf("20250101") == 2 && countOf("20250102") == 1)
    } finally {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
      testUtils.executeSql(s"DROP TABLE IF EXISTS $table CASCADE")
    }
  }

}
