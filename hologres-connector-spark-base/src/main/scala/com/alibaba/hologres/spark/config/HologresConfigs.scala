package com.alibaba.hologres.spark.config

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.OnConflictAction
import com.alibaba.hologres.spark.ConfigUtils
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil._
import org.apache.spark.SparkContext

/** Hologres config parameters process. */
class HologresConfigs(sourceOptions: Map[String, String]) extends Serializable {
  private val allConfigNames = ConfigUtils.getAllConfigNames
  sourceOptions.foreach(key => {
    if (!allConfigNames.contains(key._1)) {
      throw new IllegalArgumentException("Could not recognize parameter " + key._1)
    }
  })
  val holoConfig = new HoloConfig

  val username: String = sourceOptions.getOrElse("username",
    throw new IllegalArgumentException("Missing necessary parameter 'username'."))
  holoConfig.setUsername(username)
  val password: String = sourceOptions.getOrElse("password",
    throw new IllegalArgumentException("Missing necessary parameter 'password'."))
  holoConfig.setPassword(password)
  val enableAkv4: Boolean = sourceOptions.getOrElse("akv4_enabled", "false").toBoolean
  holoConfig.setUseAKv4(enableAkv4)
  var akv4Region: String = _
  if (enableAkv4) {
    akv4Region = sourceOptions.get("akv4_region").orNull
    holoConfig.setRegion(akv4Region)
  }

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
  val query: String = sourceOptions.getOrElse("read.query", "")
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
  val enableServerlessComputing: Boolean = sourceOptions.getOrElse("enable_serverless_computing", "false").toBoolean
  val serverlessComputingQueryPriority: Int = sourceOptions.getOrElse("serverless_computing_query_priority", "3").toInt
  val statementTimeout: Int = sourceOptions.getOrElse("statement_timeout_seconds", "28800").toInt
  sourceOptions.get("retry_count").map(v => holoConfig.setRetryCount(v.toInt))
  sourceOptions.get("retry_sleep_init_ms").map(v => holoConfig.setRetrySleepInitMs(v.toLong))
  sourceOptions.get("retry_sleep_step_ms").map(v => holoConfig.setRetrySleepStepMs(v.toLong))
  sourceOptions.get("connection_max_idle_ms").map(v => holoConfig.setConnectionMaxIdleMs(v.toLong))
  sourceOptions.get("fixed_connection_mode").map(v => holoConfig.setUseFixedFe(v.toBoolean))
  var directConnect: Boolean = sourceOptions.getOrElse("direct_connect", "false").toBoolean
  holoConfig.setEnableDirectConnection(directConnect)

  // -------------------------------------write----------------------------------------
  private val writeModeStr: String = sourceOptions.getOrElse("write.mode", "auto").toLowerCase
  var writeMode: Any = writeModeStr match {
    case "auto" => "auto"
    case "stream" => CopyMode.STREAM
    case "bulk_load" => CopyMode.BULK_LOAD
    case "bulk_load_on_conflict" => CopyMode.BULK_LOAD_ON_CONFLICT
    case "insert" => "insert"
    case _ =>
      throw new IllegalArgumentException("Could not recognize write.mode " + writeModeStr)
  }
  private val onConflictActionStr: String = sourceOptions.getOrElse("write.on_conflict_action", "insertorreplace").toLowerCase
  val onConflictAction: OnConflictAction = onConflictActionStr match {
    case "insertorignore" | "insert_or_ignore" => OnConflictAction.INSERT_OR_IGNORE
    case "insertorreplace" | "insert_or_replace" => OnConflictAction.INSERT_OR_REPLACE
    case "insertorupdate" | "insert_or_update" => OnConflictAction.INSERT_OR_UPDATE
    case _ =>
      throw new IllegalArgumentException("Could not recognize write.on_conflict_action " + onConflictActionStr)
  }
  holoConfig.setOnConflictAction(onConflictAction)
  sourceOptions.get("write.insert.dynamic_partition").map(v => holoConfig.setDynamicPartition(v.toBoolean))
  sourceOptions.get("write.insert.batch_size").map(v => holoConfig.setWriteBatchSize(v.toInt))
  sourceOptions.get("write.insert.batch_byte_size").map(v => holoConfig.setWriteBatchByteSize(v.toLong))
  sourceOptions.get("write.insert.max_interval_ms").map(v => holoConfig.setWriteMaxIntervalMs(v.toLong))
  sourceOptions.get("write.insert.thread_size").map(v => holoConfig.setWriteThreadSize(v.toInt))
  sourceOptions.get("write.insert.use_legacy_put_handler").map(v => holoConfig.setUseLegacyPutHandler(v.toBoolean))
  val writeRemoveU0000: Boolean = sourceOptions.getOrElse("write.remove_u0000", "true").toBoolean
  holoConfig.setRemoveU0000InTextColumnValue(writeRemoveU0000)
  val writeCopyFormat: String = sourceOptions.getOrElse("write.copy.format", "binary")
  val writeCopyDirtyDataCheck: Boolean = sourceOptions.getOrElse("write.copy.dirty_data_check", "false").toBoolean
  val writeCopyMaxBufferSize: Int = sourceOptions.getOrElse("write.copy.max_buffer_size", "52428800").toInt
  val writeStrictDataTypeCheck: Boolean = sourceOptions.getOrElse("write.strict_datatype_check", "false").toBoolean
  val disableRightJoinInCopy: Boolean = sourceOptions.getOrElse("write.copy.disable_right_join", "false").toBoolean
  val overWriteDropForce: Boolean = sourceOptions.getOrElse("write.overwrite_drop_force", "true").toBoolean

  // -------------------------------------read----------------------------------------
  private val readModeStr: String = sourceOptions.getOrElse("read.mode", "auto").toLowerCase
  var readMode: String = readModeStr match {
    case "auto" => "auto"
    case "bulk_read" => "bulk_read"
    case "bulk_read_compressed" => "bulk_read_compressed"
    case "select" => "select"
    case _ =>
      throw new IllegalArgumentException("Could not recognize read.mode " + readModeStr)
  }
  val readMaxTaskCount: Int = sourceOptions.getOrElse("read.max_task_count", "80").toInt
  val readPushDownPredicate: Boolean = sourceOptions.getOrElse("read.push_down_predicate", "true").toBoolean
  val readPushDownLimit: Boolean = sourceOptions.getOrElse("read.push_down_limit", "true").toBoolean
  val readSelectBatchSize: Int = sourceOptions.getOrElse("read.select.batch_size", "256").toInt
  val readSelectTimeoutSeconds: Int = sourceOptions.getOrElse("read.select.timeout_seconds", "28800").toInt
  val readCopyMaxBufferSize: Int = sourceOptions.getOrElse("read.copy.max_buffer_size", "52428800").toInt

  holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
  var sparkAppName: String = SparkContext.getOrCreate().appName
  if (sparkAppName == null || "".eq(sparkAppName)) {
    sparkAppName = "default"
  }
  var sparkAppId: String = SparkContext.getOrCreate().applicationId
  if (sparkAppId == null) {
    sparkAppId = ""
  }

  holoConfig.setAppName("hologres-connector-spark-" + sparkAppName)

  // -------------------------------------内部参数----------------------------------------
  // overwrite来自于用户对SaveMode参数的设置，写入开始会创建临时表并写入，写入成功时会清理原表的数据。
  var tempTableForOverwrite: String = _
  // 表示上游的数据已经根据holo的分布键进行了repartition
  val reshuffleByHoloDistributionKey: Boolean = sourceOptions.getOrElse("write.reshuffle_by_holo_distribution_key", "false").toBoolean
  // 与reshuffle_by_holo_distribution_key参数配合使用, 表示是否已经进行了repartition. 是则使用WRITE_V2直接写入, 否则使用WRITE_V1对DataFrame进行repartition之后再调用WRITE_V2写入
  // 内部参数, 不建议用户设置
  var needReshuffle: Boolean = sourceOptions.getOrElse("needReshuffle", "false").toBoolean

  var holoVersion: String = _

  override def clone(): HologresConfigs = new HologresConfigs(sourceOptions)
}
