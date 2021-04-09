package com.alibaba.hologres.spark.config

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.client.model.{WriteFailStrategy, WriteMode}
import com.alibaba.hologres.spark.utils.JDBCUtil._
import org.apache.commons.cli.MissingArgumentException

/** Hologres config parameters process. */
class HologresConfigs(sourceOptions: Map[String, String]) {
  val holoConfig = new HoloConfig

  val username: String = sourceOptions.getOrElse("username",
    throw new MissingArgumentException("Missing necessary parameter 'username'."))
  holoConfig.setUsername(username)
  val password: String = sourceOptions.getOrElse("password",
    throw new MissingArgumentException("Missing necessary parameter 'password'."))
  holoConfig.setPassword(password)

  lazy val database: String = sourceOptions.getOrElse("database",
    throw new MissingArgumentException("If jdbcUrl is not provided, please provide parameter 'database'."))
  lazy val endpoint: String = sourceOptions.getOrElse("endpoint",
    throw new MissingArgumentException("If jdbcUrl is not provided, please provide parameter 'endpoint'."))
  val jdbcUrl: String = sourceOptions.getOrElse("jdbcurl", getDbUrl(endpoint, database))
  holoConfig.setJdbcUrl(jdbcUrl)

  val writeMode: String = sourceOptions.getOrElse("write_mode", "insertOrIgnore").toLowerCase
  val wMode: WriteMode = writeMode match {
    case "insertorignore" | "insert_or_ignore" => WriteMode.INSERT_OR_IGNORE
    case "insertorreplace" | "insert_or_replace" => WriteMode.INSERT_OR_REPLACE
    case "insertorupdate" | "insert_or_update" => WriteMode.INSERT_OR_UPDATE
    case _ =>
      throw new MissingArgumentException("Could not recognize writeMode " + writeMode)
  }
  holoConfig.setWriteMode(wMode)

  val writeFailStrategy: String = sourceOptions.getOrElse("write_fail_strategy", "tryOneByOne").toLowerCase
  val wFailStrategy: WriteFailStrategy = writeFailStrategy match {
    case "tryonebyone" | "try_one_by_one" => WriteFailStrategy.TRY_ONE_BY_ONE
    case "none" => WriteFailStrategy.NONE
    case _ => throw new MissingArgumentException("Could not recognize WriteFailStrategy " + writeFailStrategy)
  }
  holoConfig.setWriteFailStrategy(wFailStrategy)

  sourceOptions.get("write_batch_size").map(v => holoConfig.setWriteBatchSize(v.toInt))
  sourceOptions.get("write_batch_byte_size").map(v => holoConfig.setWriteBatchByteSize(v.toLong))
  sourceOptions.get("write_max_interval_ms").map(v => holoConfig.setWriteMaxIntervalMs(v.toLong))
  sourceOptions.get("write_thread_size").map(v => holoConfig.setWriteThreadSize(v.toInt))
  sourceOptions.get("retry_count").map(v => holoConfig.setRetryCount(v.toInt))
  sourceOptions.get("retry_sleep_init_ms").map(v => holoConfig.setRetrySleepInitMs(v.toLong))
  sourceOptions.get("retry_sleep_step_ms").map(v => holoConfig.setRetrySleepStepMs(v.toLong))
  sourceOptions.get("connection_max_idle_ms").map(v => holoConfig.setConnectionMaxIdleMs(v.toLong))

  holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
}
