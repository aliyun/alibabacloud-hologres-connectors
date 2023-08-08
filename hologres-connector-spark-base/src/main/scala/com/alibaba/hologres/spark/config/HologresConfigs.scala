package com.alibaba.hologres.spark.config

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.client.model.{WriteFailStrategy, WriteMode}
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil._

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
  val jdbcUrl: String = JDBCUtil.formatUrlWithHologres(sourceOptions.getOrElse("jdbcurl", getDbUrl(endpoint, database)))
  holoConfig.setJdbcUrl(jdbcUrl)
  val table: String = sourceOptions.getOrElse("table",
    throw new IllegalArgumentException("Missing necessary parameter 'table'."))

  val writeMode: String = sourceOptions.getOrElse("write_mode", "insertOrIgnore").toLowerCase
  val wMode: WriteMode = writeMode match {
    case "insertorignore" | "insert_or_ignore" => WriteMode.INSERT_OR_IGNORE
    case "insertorreplace" | "insert_or_replace" => WriteMode.INSERT_OR_REPLACE
    case "insertorupdate" | "insert_or_update" => WriteMode.INSERT_OR_UPDATE
    case _ =>
      throw new IllegalArgumentException("Could not recognize writeMode " + writeMode)
  }
  holoConfig.setWriteMode(wMode)

  val writeFailStrategy: String = sourceOptions.getOrElse("write_fail_strategy", "tryOneByOne").toLowerCase
  val wFailStrategy: WriteFailStrategy = writeFailStrategy match {
    case "tryonebyone" | "try_one_by_one" => WriteFailStrategy.TRY_ONE_BY_ONE
    case "none" => WriteFailStrategy.NONE
    case _ => throw new IllegalArgumentException("Could not recognize WriteFailStrategy " + writeFailStrategy)
  }
  holoConfig.setWriteFailStrategy(wFailStrategy)

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
  val scan_batch_size: Int = sourceOptions.getOrElse("scan_batch_size", "256").toInt
  val scan_timeout_seconds: Int = sourceOptions.getOrElse("scan_timeout_seconds", "60").toInt
  val scan_parallelism: Int = sourceOptions.getOrElse("scan_parallelism", "10").toInt

  var copy_write_mode: Boolean = sourceOptions.getOrElse("copy_write_mode", "true").toBoolean
  val copy_write_format: String = sourceOptions.getOrElse("copy_write_format", "binary")
  val copy_write_dirty_data_check: Boolean = sourceOptions.getOrElse("copy_write_dirty_data_check", "false").toBoolean
  var copy_write_direct_connect: Boolean = sourceOptions.getOrElse("copy_write_direct_connect", "true").toBoolean

  holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
  holoConfig.setAppName("hologres-connector-spark")
}
