package com.alibaba.hologres.spark

import scala.reflect.runtime.{universe => ru}

class BaseSourceProvider() {
  val DATABASE = "database"
  val DEFAULT_DATABASE = "defaultdatabase"
  val TABLE = "table"
  val USERNAME = "username"
  val PASSWORD = "password"
  val ENDPOINT = "endpoint"
  val JDBCURL = "jdbcurl"
  val ENABLE_AKV4 = "akv4_enabled"
  val AKV4_REGION = "akv4_region"
  val DIRECT_CONNECT = "direct_connect"
  val FIXED_CONNECTION_MODE = "fixed_connection_mode"
  val CONNECTION_MAX_IDLE_MS = "connection_max_idle_ms"
  val RETRY_COUNT = "retry_count"
  val RETRY_SLEEP_INIT_MS = "retry_sleep_init_ms"
  val RETRY_SLEEP_STEP_MS = "retry_sleep_step_ms"
  val STATEMENT_TIMEOUT_SECONDS = "statement_timeout_seconds"
  val ENABLE_SERVERLESS_COMPUTING = "enable_serverless_computing"
  val SERVERLESS_COMPUTING_QUERY_PRIORITY = "serverless_computing_query_priority"
  val SERVERLESS_COMPUTING_REQUIRED_CORES = "serverless_computing_required_cores"

  // write
  val WRITE_MODE = "write.mode"
  val WRITE_REMOVE_U0000 = "write.remove_u0000"
  val RESHUFFLE_BY_HOLO_DISTRIBUTION_KEY = "write.reshuffle_by_holo_distribution_key"
  val WRITE_ENABLE_STRICT_DATATYPE_CHECK = "write.strict_datatype_check"
  val WRITE_ON_CONFLICT_ACTION = "write.on_conflict_action"
  val WRITE_OVERWRITE_DROP_FORCE = "write.overwrite_drop_force"
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
  val WRITE_COPY_DISABLE_RIGHT_JOIN = "write.copy.disable_right_join"
  val COPY_STAGE_BATCH_SIZE = "write.stage.batch_size"
  val COPY_STAGE_FILE_SIZE = "write.stage.file_size"
  val COPY_STAGE_ONLY = "write.stage.only_stage"
  val COPY_STAGE_TTL = "write.stage.ttl"
  val COPY_STAGE_COMPRESSION = "write.stage.compression"
  // 写入逻辑分区表时指定要写入的目标分区, 字段必须用双引号包裹, 字段内的双引号用两个双引号转义.
  // 列名 e.g. "ds", "kind"
  val WRITE_TARGET_PARTITION_COLUMNS = "write.target_partition_columns"
  // 分区值, 列间用逗号分隔, 多分区用分号分隔, e.g. "20250101", "100"; "20250102", "200"
  val WRITE_TARGET_PARTITION_VALUES = "write.target_partition_values"
  val WRITE_RPS_LIMIT = "write.rps_limit"

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
  // split strategy
  val SPLIT_STRATEGY = "read.split.strategy"
  val SPLIT_COLUMN = "read.split.column"
  val SPLIT_LOWER_BOUND = "read.split.lower_bound"
  val SPLIT_UPPER_BOUND = "read.split.upper_bound"
  val NUM_SPLITS = "read.split.num"
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
