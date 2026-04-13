package com.alibaba.hologres.spark3

import com.alibaba.hologres.spark.common.HoloSplitStrategyTestTrait
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * 集成测试，需要连接到真实的PostgreSQL数据库
 * 测试直接读取数据的功能
 */
@RunWith(classOf[JUnitRunner])
class SparkHoloReadSplitStrategyTest extends SparkHoloSuiteBase with HoloSplitStrategyTestTrait {

  // 添加额外的数据插入方法
  private def insertTestData(): Unit = {
    // 插入RANGE_TABLE数据
    val insertRangeSql =
      s"""
         |INSERT INTO $RANGE_TABLE (id, name, amount, created_date, created_timestamp) VALUES
         |  (1, 'name1', 100.00, '2023-01-01', '2023-01-01 10:00:00'),
         |  (2, 'name2', 200.00, '2023-01-02', '2023-01-02 11:00:00'),
         |  (3, 'name3', 300.00, '2023-01-03', '2023-01-03 12:00:00'),
         |  (4, 'name4', 400.00, '2023-01-04', '2023-01-04 13:00:00'),
         |  (5, 'name5', 500.00, '2023-01-05', '2023-01-05 14:00:00'),
         |  (6, 'name6', 600.00, '2023-01-06', '2023-01-06 15:00:00'),
         |  (7, 'name7', 700.00, '2023-01-07', '2023-01-07 16:00:00'),
         |  (8, 'name8', 800.00, '2023-01-08', '2023-01-08 17:00:00'),
         |  (9, 'name9', 900.00, '2023-01-09', '2023-01-09 18:00:00'),
         |  (10, 'name10', 1000.00, '2023-01-10', '2023-01-10 19:00:00'),
         |  (11, 'name11', 1100.00, '2023-01-11', '2023-01-11 20:00:00')
         |""".stripMargin
    testUtils.executeSql(insertRangeSql)

    // 插入PARTITION_PARENT_TABLE数据
    val insertPartitionSql =
      s"""
         |INSERT INTO $PARTITION_PARENT_TABLE (name, partition_col) VALUES
         |  ('name1', '202301'),
         |  ('name2', '202302'),
         |  ('name3', '202303')
         |""".stripMargin
    testUtils.executeSql(insertPartitionSql)

    // 插入PARTITION_PARENT_TABLE_1数据
    val insertPartition1Sql =
      s"""
         |INSERT INTO $PARTITION_PARENT_TABLE_1 (name, partition_col) VALUES
         |  ('name4', '202304'),
         |  ('name5', '202305')
         |""".stripMargin
    testUtils.executeSql(insertPartition1Sql)

    // 插入PARTITION_PARENT_TABLE_2数据（与PARTITION_PARENT_TABLE相同的数据）
    val insertPartition2Sql =
      s"""
         |INSERT INTO $PARTITION_PARENT_TABLE_2 (name, partition_col) VALUES
         |  ('name1', '202301'),
         |  ('name2', '202302'),
         |  ('name3', '202303')
         |""".stripMargin
    testUtils.executeSql(insertPartition2Sql)

    // 插入SHARD_TABLE数据
    val insertShardSql =
      s"""
         |INSERT INTO $SHARD_TABLE (id, name) VALUES
         |  (1, 'name1'),
         |  (2, 'name2'),
         |  (3, 'name3'),
         |  (4, 'name4'),
         |  (5, 'name5'),
         |  (6, 'name6'),
         |  (7, 'name7'),
         |  (8, 'name8'),
         |  (9, 'name9'),
         |  (10, 'name10')
         |""".stripMargin
    testUtils.executeSql(insertShardSql)

    // 插入SHARD_TABLE_1数据
    val insertShard1Sql =
      s"""
         |INSERT INTO $SHARD_TABLE_1 (id, name) VALUES
         |  (11, 'name11'),
         |  (12, 'name12'),
         |  (13, 'name13'),
         |  (14, 'name14'),
         |  (15, 'name15')
         |""".stripMargin
    testUtils.executeSql(insertShard1Sql)

    // 插入SHARD_TABLE_2数据（与SHARD_TABLE相同的数据）
    val insertShard2Sql =
      s"""
         |INSERT INTO $SHARD_TABLE_2 (id, name) VALUES
         |  (1, 'name1'),
         |  (2, 'name2'),
         |  (3, 'name3'),
         |  (4, 'name4'),
         |  (5, 'name5'),
         |  (6, 'name6'),
         |  (7, 'name7'),
         |  (8, 'name8'),
         |  (9, 'name9'),
         |  (10, 'name10')
         |""".stripMargin
    testUtils.executeSql(insertShard2Sql)
  }

  // 重写beforeAll方法，在设置完测试环境后插入数据
  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestUtils()
    setupTestEnvironment()
    insertTestData()
  }

  // 重写afterAll方法，清理测试环境
  override def afterAll(): Unit = {
    cleanupTestEnvironment()
  }

  def testRangeStrategy(table: String, upperBound: Int, expectedRowCount: Int): Unit = {
    // 测试简单表的范围分片
    val df = spark.read
      .format("hologres")
      .option("jdbcurl", testUtils.jdbcUrl)
      .option("username", testUtils.username)
      .option("password", testUtils.password)
      .option("table", table)
      .option("read.split.strategy", "range")
      .option("read.split.column", "id")
      .option("read.split.lower_bound", "1")
      .option("read.split.upper_bound", upperBound)
      .option("read.split.num", "3")
      .load()

    val rows = df.orderBy("id").collect()
    assert(rows.length == expectedRowCount)
    assert(rows(0).getInt(0) == 1)
    assert(rows(rows.length - 1).getInt(0) == expectedRowCount)
  }

  test("Test reading range view with range strategy") {
    testRangeStrategy(RANGE_VIEW, 10, 11)
    testRangeStrategy(RANGE_TABLE, 10, 11)
    testRangeStrategy(RANGE_VIEW, 11, 11)
    testRangeStrategy(RANGE_TABLE, 11, 11)
    testRangeStrategy(RANGE_VIEW, 100, 11)
    testRangeStrategy(RANGE_TABLE, 100, 11)
  }

  def testPartitionSameDataStrategy(table: String): Unit = {
    // 测试分区表/视图的分区分片（相同数据）
    val df = spark.read
      .format("hologres")
      .option("jdbcurl", testUtils.jdbcUrl)
      .option("username", testUtils.username)
      .option("password", testUtils.password)
      .option("table", table)
      .option("read.split.strategy", "partition")
      .option("read.split.column", "partition_col")
      .option("read.split.num", "2")
      .load()

    val rows = df.orderBy("name").collect()
    assert(rows.length == 3)
    assert(rows(0).getString(2) == "202301")
    assert(rows(2).getString(2) == "202303")
  }

  test("Test reading partition table or view with partition strategy") {
    testPartitionSameDataStrategy(PARTITION_PARENT_TABLE)
    testPartitionSameDataStrategy(PARTITION_PARENT_VIEW)
    testPartitionSameDataStrategy(PARTITION_SAME_DATA_VIEW)
  }

  test("Test reading partition different data view with partition strategy") {
    // 测试不同数据union视图的分区分片
    val df = spark.read
      .format("hologres")
      .option("jdbcurl", testUtils.jdbcUrl)
      .option("username", testUtils.username)
      .option("password", testUtils.password)
      .option("table", PARTITION_DIFFERENT_DATA_VIEW)
      .option("read.split.strategy", "partition")
      .option("read.split.column", "partition_col")
      .option("read.split.num", "2")
      .load()

    val rows = df.orderBy("name").collect()
    assert(rows.length == 5) // 应该是5行，因为是不同数据的union
    assert(rows(0).getString(2) == "202301")
    assert(rows(4).getString(2) == "202305")
  }

  def testShardStrategy(table: String, splitNum: Int, expectedRowCount: Int): Unit = {
    // 测试分片表/视图的分片策略
    val df = spark.read
      .format("hologres")
      .option("jdbcurl", testUtils.jdbcUrl)
      .option("username", testUtils.username)
      .option("password", testUtils.password)
      .option("table", table)
      .option("read.split.strategy", "shard")
      .option("read.split.num", splitNum)
      .load()

    val rows = df.orderBy("id").collect()
    assert(rows.length == expectedRowCount)
  }

  test("Test reading shard table or view with shard strategy") {
    testShardStrategy(SHARD_TABLE, 3, 10)
    testShardStrategy(SHARD_TABLE, 5, 10)
    testShardStrategy(SHARD_VIEW, 3, 10)
    testShardStrategy(SHARD_VIEW, 5, 10)
    testShardStrategy(SHARD_SAME_DATA_VIEW, 5, 10)
    testShardStrategy(SHARD_SAME_DATA_VIEW, 5, 10)
  }

  test("Test reading shard different data view with shard strategy") {
    testShardStrategy(SHARD_DIFFERENT_DATA_VIEW, 3, 15)
    testShardStrategy(SHARD_DIFFERENT_DATA_VIEW, 5, 15)
  }
}
