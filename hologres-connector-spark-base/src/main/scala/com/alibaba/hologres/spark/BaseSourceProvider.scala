package com.alibaba.hologres.spark

class BaseSourceProvider() {
  val DATABASE = "database"
  val TABLE = "table"
  val USERNAME = "username"
  val PASSWORD = "password"
  val ENDPOINT = "endpoint"
  val JDBCURL = "jdbcurl"
  val DIRECT_CONNECT = "direct_connect"
  val FIXED_CONNECTION_MODE = "fixed_connection_mode"

  val WRITE_MODE = "write_mode"
  val WRITE_BATCH_SIZE = "write_batch_size"
  val WRITE_BATCH_BYTE_SIZE = "write_batch_byte_size"
  val USE_LEGACY_PUT_HANDLER = "use_legacy_put_handler"
  val WRITE_MAX_INTERVAL_MS = "write_max_interval_ms"
  val WRITE_FAIL_STRATEGY = "write_fail_strategy"
  val WRITE_THREAD_SIZE = "write_thread_size"
  val RETRY_COUNT = "retry_count"
  val RETRY_SLEEP_INIT_MS = "retry_sleep_init_ms"
  val RETRY_SLEEP_STEP_MS = "retry_sleep_step_ms"
  val CONNECTION_MAX_IDLE_MS = "connection_max_idle_ms"
  val DYNAMIC_PARTITION = "dynamic_partition"
  val REMOVE_U0000 = "remove_u0000"

  val COPY_WRITE_MODE = "copy_write_mode"
  val COPY_WRITE_FORMAT = "copy_write_format"
  val COPY_WRITE_DIRTY_DATA_CHECK = "copy_write_dirty_data_check"
  val MAX_CELL_BUFFER_SIZE = "max_cell_buffer_size"
  val RESHUFFLE_BY_HOLO_DISTRIBUTION_KEY = "reshuffle_by_holo_distribution_key"

  val BULK_READ = "bulk_read"
  val QUERY = "query"
  val MAX_PARTITION_COUNT = "max_partition_count"
  val PUSH_DOWN_PREDICATE = "push_down_predicate"
  val PUSH_DOWN_LIMIT = "push_down_limit"
}
