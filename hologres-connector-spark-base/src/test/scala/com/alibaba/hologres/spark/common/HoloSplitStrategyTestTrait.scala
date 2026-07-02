package com.alibaba.hologres.spark.common

import com.alibaba.hologres.spark.SparkHoloTestUtils
import com.alibaba.hologres.spark.config.HologresConfigs
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.InputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * 测试分片策略的共享trait，主要创建需要的表和视图
 */
trait HoloSplitStrategyTestTrait extends BeforeAndAfterAll {
  this: Suite =>

  // 测试工具类
  protected var testUtil: SparkHoloTestUtils = _

  // 生成固定的表名后缀，确保每次运行时后缀是一样的
  private val tableSuffix: String = System.currentTimeMillis().toString

  // 测试用的表名常量
  protected val RANGE_TABLE = s"test_range_table_$tableSuffix"
  protected val RANGE_VIEW = s"test_range_view_$tableSuffix"
  protected val PARTITION_PARENT_TABLE = s"test_partition_parent_$tableSuffix"
  protected val PARTITION_PARENT_TABLE_1 = s"test_partition_parent_1_$tableSuffix"
  protected val PARTITION_PARENT_TABLE_2 = s"test_partition_parent_2_$tableSuffix"
  protected val PARTITION_PARENT_VIEW = s"test_partition_parent_view_$tableSuffix"
  protected val PARTITION_DIFFERENT_DATA_VIEW = s"test_partition_different_data_union_$tableSuffix"
  protected val PARTITION_SAME_DATA_VIEW = s"test_partition_same_data_join_$tableSuffix"
  protected val SHARD_TABLE = s"test_shard_table_$tableSuffix"
  protected val SHARD_TABLE_1 = s"test_shard_table_1_$tableSuffix"
  protected val SHARD_TABLE_2 = s"test_shard_table_2_$tableSuffix"
  protected val SHARD_VIEW = s"test_shard_view_$tableSuffix"
  protected val SHARD_DIFFERENT_DATA_VIEW = s"test_shard_different_data_union_$tableSuffix"
  protected val SHARD_SAME_DATA_VIEW = s"test_shard_same_data_join_$tableSuffix"
  protected val DIFFERENT_SHARD_COUNT_VIEW = s"test_shard_diff_count_join_$tableSuffix"

  /**
   * 初始化测试工具
   */
  protected def initializeTestUtils(): Unit = {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("setting.properties")
    val prop = new Properties()
    prop.load(inputStream)

    testUtil = new SparkHoloTestUtils()
    // Modify these parameters if don't skip the test.
    testUtil.username = prop.getProperty("USERNAME", System.getenv("HOLO_ACCESS_ID"))
    testUtil.password = prop.getProperty("PASSWORD", System.getenv("HOLO_ACCESS_KEY"))
    testUtil.jdbcUrl = prop.getProperty("JDBCURL", String.format("jdbc:postgresql://%s/%s", System.getenv("HOLO_ENDPOINT"),
      System.getenv("HOLO_TEST_DB")))
    testUtil.init()
  }

  /**
   * 创建HologresConfigs实例
   */
  protected def createHologresConfigs(table: String, extraOptions: Map[String, String] = Map()): HologresConfigs = {
    val baseOptions = Map(
      "username" -> testUtil.username,
      "password" -> testUtil.password,
      "jdbcurl" -> testUtil.jdbcUrl,
      "table" -> table
    )
    new HologresConfigs(baseOptions ++ extraOptions)
  }

  /**
   * 获取数据库连接
   */
  protected def getConnection: Connection = {
    val props = new Properties()
    props.setProperty("user", testUtil.username)
    props.setProperty("password", testUtil.password)
    DriverManager.getConnection(testUtil.jdbcUrl, props)
  }

  /**
   * 设置测试环境（创建表和视图）
   */
  protected def setupTestEnvironment(): Unit = {
    val conn = getConnection
    try {
      val stmt = conn.createStatement()

      // 创建用于RANGE分片策略的表
      stmt.execute(s"DROP TABLE IF EXISTS $RANGE_TABLE CASCADE")
      stmt.execute(
        s"""CREATE TABLE $RANGE_TABLE (
           |  id INT PRIMARY KEY,
           |  name VARCHAR(100),
           |  amount DECIMAL(10,2),
           |  created_date DATE,
           |  created_timestamp timestamptz
           |)""".stripMargin)
      stmt.execute(s"DROP VIEW IF EXISTS $RANGE_VIEW")
      stmt.execute(s"CREATE VIEW $RANGE_VIEW AS SELECT * FROM $RANGE_TABLE")

      // 创建用于PARTITION分片策略的分区表
      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202301")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202302")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202303")
      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_PARENT_VIEW")

      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE_1 CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_1}_202304")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_1}_202305")

      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE_2 CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202301")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202302")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202303")

      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_DIFFERENT_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_SAME_DATA_VIEW")

      stmt.execute(
        s"""CREATE TABLE $PARTITION_PARENT_TABLE (
           |  id SERIAL,
           |  name VARCHAR(100),
           |  partition_col VARCHAR(10)
           |) PARTITION BY LIST (partition_col)""".stripMargin)
      stmt.execute(
        s"""CREATE TABLE ${PARTITION_PARENT_TABLE}_202301 PARTITION OF $PARTITION_PARENT_TABLE
           |  FOR VALUES IN ('202301')""".stripMargin)
      stmt.execute(
        s"""CREATE TABLE ${PARTITION_PARENT_TABLE}_202302 PARTITION OF $PARTITION_PARENT_TABLE
           |  FOR VALUES IN ('202302')""".stripMargin)
      stmt.execute(
        s"""CREATE TABLE ${PARTITION_PARENT_TABLE}_202303 PARTITION OF $PARTITION_PARENT_TABLE
           |  FOR VALUES IN ('202303')""".stripMargin)
      stmt.execute(s"CREATE VIEW $PARTITION_PARENT_VIEW AS SELECT * FROM ${PARTITION_PARENT_TABLE}")

      // 另外一张分区表, 分区列与表1相同，但数据不同
      stmt.execute(
        s"""CREATE TABLE $PARTITION_PARENT_TABLE_1 (
           |  id SERIAL,
           |  name VARCHAR(100),
           |  partition_col VARCHAR(10)
           |) PARTITION BY LIST (partition_col)""".stripMargin)
      stmt.execute(s"CREATE TABLE ${PARTITION_PARENT_TABLE_1}_202304 PARTITION OF ${PARTITION_PARENT_TABLE_1} FOR VALUES IN ('202304')")
      stmt.execute(s"CREATE TABLE ${PARTITION_PARENT_TABLE_1}_202305 PARTITION OF ${PARTITION_PARENT_TABLE_1} FOR VALUES IN ('202305')")

      // 第三张分区表，分区与表1完全一致
      stmt.execute(
        s"""CREATE TABLE $PARTITION_PARENT_TABLE_2 (
           |  id SERIAL,
           |  name VARCHAR(100),
           |  partition_col VARCHAR(10)
           |) PARTITION BY LIST (partition_col)""".stripMargin)
      stmt.execute(s"CREATE TABLE ${PARTITION_PARENT_TABLE_2}_202301 PARTITION OF ${PARTITION_PARENT_TABLE_2} FOR VALUES IN ('202301')")
      stmt.execute(s"CREATE TABLE ${PARTITION_PARENT_TABLE_2}_202302 PARTITION OF ${PARTITION_PARENT_TABLE_2} FOR VALUES IN ('202302')")
      stmt.execute(s"CREATE TABLE ${PARTITION_PARENT_TABLE_2}_202303 PARTITION OF ${PARTITION_PARENT_TABLE_2} FOR VALUES IN ('202303')")

      // 包含不同数据的视图（union all）
      stmt.execute(s"CREATE VIEW $PARTITION_DIFFERENT_DATA_VIEW AS SELECT * FROM ${PARTITION_PARENT_VIEW} UNION ALL SELECT * FROM ${PARTITION_PARENT_TABLE_1}")

      // 包含相同数据的视图（join）
      stmt.execute(s"CREATE VIEW $PARTITION_SAME_DATA_VIEW AS SELECT t1.*, t2.name as t2_name " +
        s"FROM ${PARTITION_PARENT_VIEW} t1 JOIN ${PARTITION_PARENT_TABLE_2} t2 ON t1.partition_col = t2.partition_col")


      // 创建用于SHARD分片策略的表
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE")
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE_1")
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE_2")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_DIFFERENT_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_SAME_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $DIFFERENT_SHARD_COUNT_VIEW")

      try {
        stmt.execute("call hg_create_table_group('tg_20', 20)")
      } catch {
        case _: Exception =>
      }
      try {
        stmt.execute("call hg_create_table_group('tg_20_a', 20)")
      } catch {
        case _: Exception =>
      }
      stmt.execute(
        s"""CREATE TABLE $SHARD_TABLE (
           |  id int PRIMARY KEY,
           |  name VARCHAR(100)
           |) WITH (
           | table_group='tg_20'
           |)
           |""".stripMargin)
      stmt.execute(
        s"""CREATE TABLE $SHARD_TABLE_1 (
           |  id int PRIMARY KEY,
           |  name VARCHAR(100)
           |) WITH (
           | table_group='tg_20_a'
           |)
           |""".stripMargin)
      stmt.execute(
        s"""CREATE TABLE $SHARD_TABLE_2 (
           |  id int PRIMARY KEY,
           |  name VARCHAR(100)
           |) WITH (
           | table_group='tg_20_a'
           |)
           |""".stripMargin)
      stmt.execute(s"CREATE VIEW $SHARD_VIEW AS SELECT * FROM $SHARD_TABLE")
      stmt.execute(s"CREATE VIEW $SHARD_DIFFERENT_DATA_VIEW AS SELECT * FROM $SHARD_VIEW UNION ALL SELECT * FROM $SHARD_TABLE_1")
      stmt.execute(s"CREATE VIEW $SHARD_SAME_DATA_VIEW AS SELECT t1.*, t2.name as t2_name FROM $SHARD_VIEW t1 JOIN $SHARD_TABLE_2 t2 ON t1.id = t2.id")
      stmt.execute(s"CREATE VIEW $DIFFERENT_SHARD_COUNT_VIEW AS SELECT t1.*, t2.name as t2_name FROM $SHARD_TABLE t1 JOIN $RANGE_TABLE t2 ON t1.id = t2.id")
      stmt.close()
    } finally {
      conn.close()
    }
  }

  /**
   * 清理测试环境
   */
  protected def cleanupTestEnvironment(): Unit = {
    val conn = getConnection
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"DROP TABLE IF EXISTS $RANGE_TABLE CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS $RANGE_VIEW")
      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE_1 CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS $PARTITION_PARENT_TABLE_2 CASCADE")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202301")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202302")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE}_202303")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_1}_202304")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_1}_202305")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202301")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202302")
      stmt.execute(s"DROP TABLE IF EXISTS ${PARTITION_PARENT_TABLE_2}_202303")
      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_PARENT_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_DIFFERENT_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $PARTITION_SAME_DATA_VIEW")
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE")
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE_1")
      stmt.execute(s"DROP TABLE IF EXISTS $SHARD_TABLE_2")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_DIFFERENT_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $SHARD_SAME_DATA_VIEW")
      stmt.execute(s"DROP VIEW IF EXISTS $DIFFERENT_SHARD_COUNT_VIEW")
      stmt.close()
    } finally {
      conn.close()
    }
  }
}
