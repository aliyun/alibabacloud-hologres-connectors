package com.alibaba.hologres.spark3

class SparkHoloTableCatalogSuite extends SparkHoloSuiteBase {

  test("Holo Table Catalog Test") {
    spark.conf.set("spark.sql.catalog.hologres_external", "com.alibaba.hologres.spark3.HoloTableCatalog")
    spark.conf.set("spark.sql.catalog.hologres_external.username", testUtils.username)
    spark.conf.set("spark.sql.catalog.hologres_external.password", testUtils.password)
    spark.conf.set("spark.sql.catalog.hologres_external.jdbcurl", testUtils.jdbcUrl)
    spark.conf.set("spark.sql.catalog.hologres_external.max_partition_count", 2)
    var defaultNamespace = ""
    val pattern = "(.*://[^/]+/)([^?]+)(.*)".r
    testUtils.jdbcUrl match {
      case pattern(_, database, _) => defaultNamespace = database
      case _ =>
        throw new IllegalArgumentException("Invalid JDBC URL format, " + testUtils.jdbcUrl)
    }
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
    if (!tables.sameElements(spark.sql(s"show tables in $defaultNamespace;").select("tableName").collect())) {
      throw new Exception("should not happen!")
    }
    var count = 0
    for (table <- tables) {
      if (table.getString(0) == s"`public.$table1`" || table.getString(0) == s"`public.$table2`") {
        count = count + 1
      }
    }
    if (count != 2) {
      throw new Exception(s"$table1 or $table2 not found!")
    }

    val res1 = spark.sql(s"select * from `public.$table1`;").orderBy("id").cache()
    val res2 = spark.sql(s"select * from $defaultNamespace.`public.$table1`;").orderBy("id").cache()
    spark.sql(s"insert into `public.$table2` select * from `public.$table1`;")
    val res3 = spark.sql(s"select * from $defaultNamespace.`public.$table2`;").orderBy("id").cache()

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

}
