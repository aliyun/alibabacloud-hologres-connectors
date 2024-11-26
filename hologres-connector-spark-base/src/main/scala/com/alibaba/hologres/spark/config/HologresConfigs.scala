package com.alibaba.hologres.spark.config

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.{WriteFailStrategy, WriteMode}
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil._
import org.apache.spark.SparkContext

/** Hologres config parameters process. */
class HologresConfigs(sourceOptions: Map[String, String]) extends Serializable {
  val holoConfig = new HoloConfig

  val username: String = sourceOptions.getOrElse("username",
    throw new IllegalArgumentException("Missing necessary parameter 'username'."))
  holoConfig.setUsername(username)
  val password: String = sourceOptions.getOrElse("password",
    throw new IllegalArgumentException("Missing necessary parameter 'password'."))
  holoConfig.setPassword(password)

  lazy val database: String = sourceOptions.getOrElse("database",
    throw new IllegalArgumentException("If jdbcUrl is not provided, please provide parameter 'database'."))
  lazy val endpoint: String = sourceOptions.getOrElse("endpoint",
    throw new IllegalArgumentException("If jdbcUrl is not provided, please provide parameter 'endpoint'."))
  var jdbcUrl: String = JDBCUtil.formatUrlWithHologres(sourceOptions.getOrElse("jdbcurl", getDbUrl(endpoint, database)))
  holoConfig.setJdbcUrl(jdbcUrl)

  def resetJdbcUrl(url: String): Unit = {
    jdbcUrl = url
    holoConfig.setJdbcUrl(jdbcUrl)
  }

  // when read from holo, could choose set query or table
  val query: String = sourceOptions.getOrElse("query", "")
  var table: String = sourceOptions.getOrElse("table", "")
  lazy val isTableSource: Boolean = {
    if ((query == null || query.isEmpty) && (table == null || table.isEmpty)) {
      throw new IllegalArgumentException("Missing necessary parameter 'table'. If table is not provided, please provide parameter 'query' for read.")
    }
    if ((query != null && query.nonEmpty) && (table != null && table.nonEmpty)) {
      throw new IllegalArgumentException("If query is provided, please do not provide parameter 'table'.")
    }
    table != null && table.nonEmpty
  }

  private val writeModeStr: String = sourceOptions.getOrElse("write_mode", "insertOrIgnore").toLowerCase
  val writeMode: WriteMode = writeModeStr match {
    case "insertorignore" | "insert_or_ignore" => WriteMode.INSERT_OR_IGNORE
    case "insertorreplace" | "insert_or_replace" => WriteMode.INSERT_OR_REPLACE
    case "insertorupdate" | "insert_or_update" => WriteMode.INSERT_OR_UPDATE
    case _ =>
      throw new IllegalArgumentException("Could not recognize writeMode " + writeModeStr)
  }
  holoConfig.setWriteMode(writeMode)

  val writeFailStrategy: String = sourceOptions.getOrElse("write_fail_strategy", "tryOneByOne").toLowerCase
  val wFailStrategy: WriteFailStrategy = writeFailStrategy match {
    case "tryonebyone" | "try_one_by_one" => WriteFailStrategy.TRY_ONE_BY_ONE
    case "none" => WriteFailStrategy.NONE
    case _ => throw new IllegalArgumentException("Could not recognize WriteFailStrategy " + writeFailStrategy)
  }
  holoConfig.setWriteFailStrategy(wFailStrategy)

  val enableServerlessComputing: Boolean = sourceOptions.getOrElse("enable_serverless_computing", "false").toBoolean
  val serverlessComputingQueryPriority: Int = sourceOptions.getOrElse("serverless_computing_query_priority", "3").toInt
  val statementTimeout: Int = sourceOptions.getOrElse("statement_timeout", "28800000").toInt
  sourceOptions.get("dynamic_partition").map(v => holoConfig.setDynamicPartition(v.toBoolean))
  sourceOptions.get("write_batch_size").map(v => holoConfig.setWriteBatchSize(v.toInt))
  sourceOptions.get("write_batch_byte_size").map(v => holoConfig.setWriteBatchByteSize(v.toLong))
  sourceOptions.get("write_max_interval_ms").map(v => holoConfig.setWriteMaxIntervalMs(v.toLong))
  sourceOptions.get("write_thread_size").map(v => holoConfig.setWriteThreadSize(v.toInt))
  sourceOptions.get("use_legacy_put_handler").map(v => holoConfig.setUseLegacyPutHandler(v.toBoolean))
  sourceOptions.get("retry_count").map(v => holoConfig.setRetryCount(v.toInt))
  sourceOptions.get("retry_sleep_init_ms").map(v => holoConfig.setRetrySleepInitMs(v.toLong))
  sourceOptions.get("retry_sleep_step_ms").map(v => holoConfig.setRetrySleepStepMs(v.toLong))
  sourceOptions.get("connection_max_idle_ms").map(v => holoConfig.setConnectionMaxIdleMs(v.toLong))
  sourceOptions.get("fixed_connection_mode").map(v => holoConfig.setUseFixedFe(v.toBoolean))
  val removeU0000: Boolean = sourceOptions.getOrElse("remove_u0000", "true").toBoolean
  holoConfig.setRemoveU0000InTextColumnValue(removeU0000)
  val scan_batch_size: Int = sourceOptions.getOrElse("scan_batch_size", "256").toInt
  val scan_timeout_seconds: Int = sourceOptions.getOrElse("scan_timeout_seconds", "28800").toInt
  val max_partition_count: Int = sourceOptions.getOrElse("max_partition_count", "80").toInt
  val pushDownPredicate: Boolean = sourceOptions.getOrElse("push_down_predicate", "true").toBoolean
  val pushDownLimit: Boolean = sourceOptions.getOrElse("push_down_limit", "true").toBoolean

  // 调整之前两个boolean类型的参数copy_write_mode和bulk_load为新的enum参数
  private val copyModeStr: String = sourceOptions.getOrElse("copy_write_mode", "stream").toLowerCase
  var copyMode: CopyMode = copyModeStr match {
    case "stream" => CopyMode.STREAM
    case "bulk_load" => CopyMode.BULK_LOAD
    case "bulk_load_on_conflict" => CopyMode.BULK_LOAD_ON_CONFLICT
    case "disable" => null
    case _ =>
      throw new IllegalArgumentException("Could not recognize copy_write_mode " + copyModeStr)
  }
  val copy_write_format: String = sourceOptions.getOrElse("copy_write_format", "binary")
  val copy_write_dirty_data_check: Boolean = sourceOptions.getOrElse("copy_write_dirty_data_check", "false").toBoolean
  var direct_connect: Boolean = sourceOptions.getOrElse("direct_connect", "true").toBoolean
  val max_cell_buffer_size: Int = sourceOptions.getOrElse("max_cell_buffer_size", "20971520").toInt
  val reshuffleByHoloDistributionKey: Boolean = sourceOptions.getOrElse("reshuffle_by_holo_distribution_key", "false").toBoolean

  val bulkRead: Boolean = sourceOptions.getOrElse("bulk_read", "true").toBoolean
  // overwrite来自于用户对SaveMode参数的设置，写入开始会创建临时表并写入，写入成功时会清理原表的数据。
  var tempTableForOverwrite: String = _

  holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
  var sparkAppName: String = SparkContext.getOrCreate().appName
  if (sparkAppName == null || "".eq(sparkAppName)) {
    sparkAppName = "default"
  }
  holoConfig.setAppName("hologres-connector-spark-" + sparkAppName)

  override def clone(): HologresConfigs = new HologresConfigs(sourceOptions)
}
