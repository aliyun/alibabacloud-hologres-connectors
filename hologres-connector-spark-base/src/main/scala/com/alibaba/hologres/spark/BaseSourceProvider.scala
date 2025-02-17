package com.alibaba.hologres.spark
import scala.reflect.runtime.{universe => ru}

class BaseSourceProvider() {
  val DATABASE = "database"
  val TABLE = "table"
  val USERNAME = "username"
  val PASSWORD = "password"
  val ENDPOINT = "endpoint"
  val JDBCURL = "jdbcurl"
  val DIRECT_CONNECT = "direct_connect"
  val FIXED_CONNECTION_MODE = "fixed_connection_mode"
  val CONNECTION_MAX_IDLE_MS = "connection_max_idle_ms"
  val RETRY_COUNT = "retry_count"
  val RETRY_SLEEP_INIT_MS = "retry_sleep_init_ms"
  val RETRY_SLEEP_STEP_MS = "retry_sleep_step_ms"
  val STATEMENT_TIMEOUT_SECONDS = "statement_timeout_seconds"
  val ENABLE_SERVERLESS_COMPUTING = "enable_serverless_computing"
  val SERVERLESS_COMPUTING_QUERY_PRIORITY = "serverless_computing_query_priority"

  // write
  val WRITE_MODE = "write.mode"
  val WRITE_REMOVE_U0000 = "write.remove_u0000"
  val RESHUFFLE_BY_HOLO_DISTRIBUTION_KEY = "write.reshuffle_by_holo_distribution_key"
  val WRITE_ON_CONFLICT_ACTION = "write.on_conflict_action"
  // write insert
  val WRITE_INSERT_BATCH_SIZE = "write.insert.batch_size"
  val WRITE_INSERT_BATCH_BYTE_SIZE = "write.insert.batch_byte_size"
  val WRITE_INSERT_USE_LEGACY_PUT_HANDLER = "write.insert.use_legacy_put_handler"
  val WRITE_INSERT_MAX_INTERVAL_MS = "write.insert.max_interval_ms"
  val WRITE_INSERT_THREAD_SIZE = "write.insert.thread_size"
  val WRITE_INSERT_DYNAMIC_PARTITION = "write.insert.dynamic_partition"
  // write copy
  val WRITE_COPY_FORMAT = "write.copy.format"
  val WRITE_COPY_DIRTY_DATA_CHECK = "write.copy.dirty_data_check"
  val WRITE_COPY_MAX_BUFFER_SIZE = "write.copy.max_buffer_size"

  // read
  val READ_MODE = "read.mode"
  val READ_QUERY = "read.query"
  val READ_MAX_TASK_COUNT = "read.max_task_count"
  val READ_PUSH_DOWN_PREDICATE = "read.push_down_predicate"
  val READ_PUSH_DOWN_LIMIT = "read.push_down_limit"
  // read scan
  val READ_SELECT_BATCH_SIZE = "read.select.batch_size"
  val READ_SELECT_TIMEOUT_SECONDS = "read.select.timeout_seconds"
  // read copy
  val READ_COPY_MAX_BUFFER_SIZE = "read.copy.max_buffer_size"
}

object ConfigUtils {
  private lazy val cachedFieldValues: List[String] = computeAllConfigNames(new BaseSourceProvider)

  private def computeAllConfigNames(obj: Any): List[String] = {
    val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)
    val instanceMirror = mirror.reflect(obj)
    val classSymbol = mirror.classSymbol(obj.getClass)
    val members = classSymbol.toType.members
    val values = members.collect {
      case m if m.isMethod && m.asMethod.isGetter && m.isPublic =>
        instanceMirror.reflectField(m.asTerm).get.asInstanceOf[String]
    }
    values.toList
  }

  def getAllConfigNames: List[String] = cachedFieldValues
}
