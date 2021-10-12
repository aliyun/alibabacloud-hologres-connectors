package com.alibaba.hologres.spark.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.spark.config.HologresConfigs

class BaseSourceProvider() {
  val DATABASE = "database"
  val TABLE = "table"
  val USERNAME = "username"
  val PASSWORD = "password"
  val ENDPOINT = "endpoint"
  val JDBCURL = "jdbcurl"
  val WRITE_MODE = "write_mode"
  val WRITE_BATCH_SIZE = "write_batch_size"
  val WRITE_BATCH_BYTE_SIZE = "write_batch_byte_size"
  val REWRITE_SQL_MAX_BATCH_SIZE = "rewrite_sql_max_batch_size"
  val WRITE_MAX_INTERVAL_MS = "write_max_interval_ms"
  val WRITE_FAIL_STRATEGY = "write_fail_strategy"
  val WRITE_THREAD_SIZE = "write_thread_size"
  val RETRY_COUNT = "retry_count"
  val RETRY_SLEEP_INIT_MS = "retry_sleep_init_ms"
  val RETRY_SLEEP_STEP_MS = "retry_sleep_step_ms"
  val CONNECTION_MAX_IDLE_MS = "connection_max_idle_ms"

  def getOrCreateHoloClient(sourceOptions: Map[String, String]): HoloClient = {
    val hologresConfigs: HologresConfigs = new HologresConfigs(sourceOptions)

    val holoClient = new HoloClient(hologresConfigs.holoConfig)
    holoClient.setAsyncCommit(true)

    holoClient
  }
}
