package com.alibaba.hologres.spark.config

import com.alibaba.hologres.client.HoloConfig
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.OnConflictAction
import com.alibaba.hologres.client.utils.CommonUtil
import com.alibaba.hologres.spark.ConfigUtils
import com.alibaba.hologres.spark.utils.JDBCUtil
import com.alibaba.hologres.spark.utils.JDBCUtil._

/** Hologres config parameters process. */
class HologresConfigs(sourceOptions: Map[String, String], val sparkAppName: String = "default", val sparkAppId: String = "") extends Serializable {
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

  // when read from holo, could choose set query or table(view)
  val query: String = sourceOptions.getOrElse("read.query", "")
  var table: String = sourceOptions.getOrElse("table", "")
  lazy val isTableConfigured: Boolean = {
    if ((query == null || query.isEmpty) && (table == null || table.isEmpty)) {
      throw new IllegalArgumentException("Missing necessary parameter 'table'. If table is not provided, please provide parameter 'query' for read.")
    }
    if ((query != null && query.nonEmpty) && (table != null && table.nonEmpty)) {
      throw new IllegalArgumentException("If query is provided, please do not provide parameter 'table'.")
    }
    table != null && table.nonEmpty
  }
  // sourceType: TABLE,VIEW,QUERY
  var sourceType: String = "TABLE"
  val enableServerlessComputing: Boolean = sourceOptions.getOrElse("enable_serverless_computing", "false").toBoolean
  val serverlessComputingQueryPriority: Int = sourceOptions.getOrElse("serverless_computing_query_priority", "3").toInt
  val serverlessComputingRequiredCores: Int = sourceOptions.getOrElse("serverless_computing_required_cores", "0").toInt
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
    case "stage" => "stage"
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
  val copyStageBatchSize: Int = sourceOptions.getOrElse("write.stage.batch_size", "8192").toInt
  val copyStageFileSize: Int = sourceOptions.getOrElse("write.stage.file_size", "67108864").toInt
  // 保留stage,不写入真正的表
  val copyStageOnly: Boolean = sourceOptions.getOrElse("write.stage.only_stage", "false").toBoolean
  // 仅only_stage时才可以设置ttl
  val copyStageTtl: Int = sourceOptions.getOrElse("write.stage.ttl", "7200").toInt
  // stage模式写入时是否启用Arrow LZ4压缩（依赖holo-client >= 2.7.6）
  val copyStageCompression: Boolean = sourceOptions.getOrElse("write.stage.compression", "false").toBoolean
  // 逻辑分区表写入时指定目标分区
  // write.target_partition_columns: 形如 "ds" 或 "ds", "kind"
  // write.target_partition_values: 列间逗号分隔, 多分区分号分隔, 形如 "20250101" 或 "20250101", "100"; "20250102", "200"
  // 字段必须用双引号包裹, 字段内的双引号用两个双引号转义. 详见 CommonUtil.parseLogicalPartitionColumn*
  val (writeTargetPartitionColumns: Array[String],
       writeTargetPartitionValues: Array[Array[String]]) = {
    val rawCols = sourceOptions.get("write.target_partition_columns").map(_.trim).filter(_.nonEmpty)
    val rawVals = sourceOptions.get("write.target_partition_values").map(_.trim).filter(_.nonEmpty)
    (rawCols, rawVals) match {
      case (None, None) =>
        (Array.empty[String], Array.empty[Array[String]])
      case (Some(_), None) | (None, Some(_)) =>
        throw new IllegalArgumentException(
          "write.target_partition_columns and write.target_partition_values must be set together")
      case (Some(cn), Some(vs)) =>
        val parsedCols = CommonUtil.parseLogicalPartitionColumnNames(cn)
        val parsedVals = CommonUtil.parseLogicalPartitionColumnValues(vs)
        parsedVals.zipWithIndex.foreach { case (row, idx) =>
          if (row.length != parsedCols.length) {
            throw new IllegalArgumentException(
              s"write.target_partition_values row #$idx has ${row.length} fields, " +
                s"but write.target_partition_columns has ${parsedCols.length} columns. " +
                s"target_partition_columns='$cn', target_partition_values='$vs'")
          }
        }
        (parsedCols, parsedVals)
    }
  }
  sourceOptions.get("write.rps_limit").map(v => holoConfig.setWriteRps(v.toInt))

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
  val readPushDownPredicate: Boolean = sourceOptions.getOrElse("read.push_down_predicate", "true").toBoolean
  val readPushDownLimit: Boolean = sourceOptions.getOrElse("read.push_down_limit", "true").toBoolean
  val readSelectBatchSize: Int = sourceOptions.getOrElse("read.select.batch_size", "256").toInt
  val readSelectTimeoutSeconds: Int = sourceOptions.getOrElse("read.select.timeout_seconds", "28800").toInt
  val readCopyMaxBufferSize: Int = sourceOptions.getOrElse("read.copy.max_buffer_size", "52428800").toInt

  // -------------------------------------view split parameters----------------------------------------
  val splitStrategy: String = sourceOptions.getOrElse("read.split.strategy", "shard")
  val splitColumn: String = sourceOptions.getOrElse("read.split.column", "")
  val splitLowerBound: String = sourceOptions.getOrElse("read.split.lower_bound", "")
  val splitUpperBound: String = sourceOptions.getOrElse("read.split.upper_bound", "")
  val numSplits: Int = sourceOptions.getOrElse("read.split.num", sourceOptions.getOrElse("read.max_task_count", "80")).toInt

  holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
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

  override def clone(): HologresConfigs = new HologresConfigs(sourceOptions, sparkAppName, sparkAppId)
}
