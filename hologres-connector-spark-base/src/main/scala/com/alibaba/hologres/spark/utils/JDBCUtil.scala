package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin
import com.alibaba.hologres.client.copy.CopyUtil
import com.alibaba.hologres.client.function.FunctionWithSQLException
import com.alibaba.hologres.client.impl.util.ConnectionUtil
import com.alibaba.hologres.client.model.{HoloVersion, OnConflictAction, TableName, TableSchema}
import com.alibaba.hologres.client.utils.IdentifierUtil
import com.alibaba.hologres.client.{Command, HoloClient}
import com.alibaba.hologres.org.postgresql.PGProperty
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.commons.lang3.exception.ExceptionUtils

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import java.util
import java.util.Properties

/** JDBC utils. */
object JDBCUtil {
  private val logger = new LoggerWrapper(getClass)

  def getDbUrl(endpoint: String, database: String): String = {
    if (!checkEndpoint(endpoint)) {
      throw new IllegalArgumentException("Format error of parameter 'endpoint'.")
    }

    "jdbc:postgresql://" + endpoint + "/" + database
  }

  // end with Port and not start with ':'
  def checkEndpoint(endpoint: String): Boolean = {
    val portPatten = ("(.+):(([1-9]([0-9]{0,4}))|([1-6][0-5][0-5][0-3][0-5]))$").r

    1 == portPatten.findAllIn(endpoint).size
  }

  def formatUrlWithHologres(oldUrl: String) = {
    var url = oldUrl
    // the copyWriter just supports jdbc:hologres
    if (oldUrl != null && oldUrl.startsWith("jdbc:postgresql:")) {
      url = "jdbc:hologres:" + oldUrl.substring("jdbc:postgresql:".length)
    }
    url
  }

  def replaceUrlDatabase(jdbcUrl: String, newDatabase: String): String = {
    val pattern = "(.*://[^/]+/)([^?]+)(.*)".r

    jdbcUrl match {
      case pattern(prefix, _, suffix) => s"${prefix}${newDatabase}${suffix}"
      case _ =>
        throw new IllegalArgumentException("Invalid JDBC URL format, " + jdbcUrl)
    }
  }

  def setAkv4Region(info: Properties, akv4Region: String): Any = {
    PGProperty.USER.set(info, AKv4AuthenticationPlugin.AKV4_PREFIX + PGProperty.USER.get(info))
    PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(info, classOf[AKv4AuthenticationPlugin].getName)
    if (akv4Region != null && akv4Region.nonEmpty) info.setProperty(AKv4AuthenticationPlugin.REGION, akv4Region)
  }

  def couldDirectConnect(configs: HologresConfigs): Boolean = {
    val url = configs.jdbcUrl
    val info = new Properties
    PGProperty.USER.set(info, configs.username)
    PGProperty.PASSWORD.set(info, configs.password)
    PGProperty.APPLICATION_NAME.set(info, "hologres-connector-spark_copy")
    if (configs.enableAkv4) {
      setAkv4Region(info, configs.akv4Region)
    }
    val directUrl = ConnectionUtil.getDirectConnectionUrl(url, info, false)
    !directUrl.equals(url)
  }


  object getHoloVersion extends FunctionWithSQLException[Connection, HoloVersion] {
    override def apply(conn: Connection): HoloVersion = {
      ConnectionUtil.getHoloVersion(conn)
    }
  }

  def getSimpleSelectFromTable(table: String, selectFields: Array[String]): String = {
    val selectExpressions: String = selectFields.map(field => "\"" + field + "\"").mkString(", ")
    s"select $selectExpressions from $table"
  }

  def getSimpleSelectFromQuery(query: String, selectFields: Array[String]): String = {
    val selectExpressions: String = selectFields.map(field => "\"" + field + "\"").mkString(", ")
    s"select $selectExpressions from ($query) t"
  }

  def createConnection(hologresConfigs: HologresConfigs, database: String = null, simple: Boolean = false): Connection = {
    try Class.forName("com.alibaba.hologres.org.postgresql.Driver")
    catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
    var url = hologresConfigs.jdbcUrl
    try {
      val info: Properties = new Properties
      PGProperty.USER.set(info, hologresConfigs.username)
      PGProperty.PASSWORD.set(info, hologresConfigs.password)
      PGProperty.SOCKET_TIMEOUT.set(info, 3600)
      PGProperty.APPLICATION_NAME.set(info, "spark_connector_util")
      if (simple) {
        PGProperty.PREFER_QUERY_MODE.set(info, "simple")
      }
      if (hologresConfigs.enableAkv4) {
        setAkv4Region(info, hologresConfigs.akv4Region)
      }
      if (database != null) {
        url = replaceUrlDatabase(hologresConfigs.jdbcUrl, database)
      }
      if (hologresConfigs.directConnect) {
        url = ConnectionUtil.getDirectConnectionUrl(url, info, false)
      }
      val conn = DriverManager.getConnection(url, info)
      logger.info(s"create connection to holo with url $url")
      conn
    } catch {
      case e: SQLException =>
        throw new RuntimeException(String.format("Failed getting connection to %s because %s", url, ExceptionUtils.getStackTrace(e)))
    }
  }

  def executeSql(conn: Connection, sql: String): Unit = {
    executeSql(conn, sql, ignoreException = false)
  }

  def executeSql(conn: Connection, sql: String, ignoreException: Boolean): Unit = {
    try {
      val stmt = conn.createStatement()
      stmt.execute(sql)
      stmt.close()
      logger.info("execute sql success: " + sql)
    } catch {
      case e: SQLException =>
        logger.error("execute sql failed: " + sql, e)
        if (!ignoreException) {
          throw new RuntimeException(e)
        }
    }
  }

  def listDatabases(hologresConfigs: HologresConfigs): Array[String] = {
    var databases = Array.empty[String]
    val sql =
      s"""
    SELECT
    b.datname AS "dbname",
    has_database_privilege(b.datname, 'CONNECT') AS "connect"
    FROM (
      SELECT *
        FROM pg_db_role_setting a
        WHERE a.setrole = 0
        UNION ALL
        SELECT b.oid, 0, ARRAY['']
        FROM pg_database b
        WHERE b.oid NOT IN (
      SELECT setdatabase
        FROM pg_db_role_setting
    )
    ) a
    JOIN pg_database b
    ON (a.setdatabase = b.oid)
    WHERE b.datname NOT IN ('postgres', 'template0', 'template1', 'holo_sys_admin', 'pgconn_history');
    """
    val conn = createConnection(hologresConfigs)
    try {
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(sql)
      while (resultSet.next) {
        if (resultSet.getString(2).equals("t")) {
          databases :+= resultSet.getString(1)
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(String.format("Fail to list databases"), e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
    databases
  }

  def listSchemas(hologresConfigs: HologresConfigs, database: String = null): Array[String] = {
    var schemas = Array.empty[String]
    val conn = createConnection(hologresConfigs, database)
    try {
      val listSchemasSql =
        s"""
        SELECT
          schema_name
        FROM
          information_schema.schemata
        WHERE
          schema_name NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'pg_catalog', 'information_schema',
           'hologres', 'hologres_statistic', 'hologres_sample', 'hologres_streaming_mv', 'hg_recyclebin');
        """
      val stmt = conn.createStatement()
      val resultSet = stmt.executeQuery(listSchemasSql)
      while (resultSet.next) {
        schemas :+= resultSet.getString(1)
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(String.format("Fail to list schemas in the database %s.", database), e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
    schemas
  }

  def listTables(hologresConfigs: HologresConfigs, schema: String, database: String = null): Array[String] = {
    var tables = Array.empty[String]
    val conn = createConnection(hologresConfigs, database)
    try {
      // get all schemas
      val schemas = listSchemas(hologresConfigs, database)
      if (schema != null && !schemas.contains(schema)) {
        throw new IllegalArgumentException(s"The schema $schema is not exist.")
      }
      // get all tables
      val listTablesSql =
        s"""
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE
            table_type = 'BASE TABLE'
            AND table_schema = '%s'
        ORDER BY
            table_type,
            table_name;
        """
      if (schema != null) {
        val stmt = conn.createStatement()
        val resultSet = stmt.executeQuery(listTablesSql.format(schema))
        while (resultSet.next) {
          tables :+= s"${resultSet.getString(1)}"
        }
      } else {
        for (schema <- schemas) {
          val stmt = conn.createStatement()
          val resultSet = stmt.executeQuery(listTablesSql.format(schema))
          while (resultSet.next) {
            tables :+= s"$schema.${resultSet.getString(1)}"
          }
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(String.format("Fail to list schemas in the database %s.", database), e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
    tables
  }

  def generateTempTableNameForOverwrite(hologresConfigs: HologresConfigs): String = {
    val tableName: TableName = TableName.valueOf(hologresConfigs.table)
    val tempTableName = String.format("tmp_s2h_ow_%s_%s_%s"
      , System.currentTimeMillis.toString
      , hologresConfigs.holoConfig.getAppName
      , tableName.getTableName)

    TableName.valueOf(
      IdentifierUtil.quoteIdentifier(tableName.getSchemaName, true),
      // holo表名长度限制127个字符
      IdentifierUtil.quoteIdentifier(tempTableName.substring(0, math.min(tempTableName.length, 127)), true)
    ).getFullName
  }

  private def getDropTableForceSuffix(hologresConfigs: HologresConfigs): String = {
    // 3.1版本起默认开启回收站, 但我们创建的临时表,或者写入overwrite成功的旧表没必要进回收站
    val RECYCLEBIN_HOLO_VERSION = new HoloVersion(3, 1, 0)

    if (hologresConfigs.holoVersion != null
      && new HoloVersion(hologresConfigs.holoVersion).compareTo(RECYCLEBIN_HOLO_VERSION) >= 0
      && hologresConfigs.overWriteDropForce) {
      "FORCE"
    } else {
      ""
    }
  }

  /**
   * create temp table for overwrite
   */
  def createTempTableForOverWrite(hologresConfigs: HologresConfigs): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)
    val suffix = getDropTableForceSuffix(hologresConfigs)
    /*
    BEGIN ;
    -- 清理潜在的临时表
    DROP TABLE IF EXISTS <table_new>;
    -- 创建临时表
    SET hg_experimental_enable_create_table_like_properties=on;
    CALL HG_CREATE_TABLE_LIKE ('<table_new>', 'select * from <table>');
    COMMIT ;
    -- 更新统计信息
    ANALYZE <table_new>;
    */
    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      val statement = conn.createStatement()
      val sql = String.format("-- From Spark-Connector: <create temp table for overwrite>\n"
        + "set hg_experimental_force_sync_replay=on;\n"
        + "BEGIN;\n"
        + "DROP TABLE IF EXISTS %s %s;\n"
        + "set hg_experimental_enable_create_table_like_properties=on;\n"
        + "CALL HG_CREATE_TABLE_LIKE ('%s', 'select * from %s');\n"
        + "COMMIT;"
        + "ANALYZE %s;", hologresConfigs.tempTableForOverwrite, suffix, hologresConfigs.tempTableForOverwrite,
        hologresConfigs.table, hologresConfigs.tempTableForOverwrite)
      logger.info(s"create temp table for overwrite DDL: \n$sql")
      statement.execute(sql)
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * replace the original table with the temp table when overwrite success
   */
  def renameTempTableForOverWrite(hologresConfigs: HologresConfigs, parentTable: String = null, partitionValue: String = null): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)
    val suffix = getDropTableForceSuffix(hologresConfigs)
    /*
    BEGIN ;
    -- 删除旧表
    DROP TABLE IF EXISTS  <table>;
    -- 临时表改名
    ALTER TABLE <table_new> RENAME TO <table>;
    COMMIT ;
    */
    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      val statement = conn.createStatement()
      var sql: String = null
      val tableName: TableName = TableName.valueOf(hologresConfigs.table)
      val onlyTablename = IdentifierUtil.quoteIdentifier(tableName.getTableName)
      if (partitionValue == null || parentTable == null) {
        sql = String.format("-- From Spark-Connector: <replace the original table with the temp table when overwrite success>\n"
          + "set hg_experimental_force_sync_replay=on;\n"
          + "BEGIN;\n"
          + "DROP TABLE IF EXISTS %s %s;\n"
          + "ALTER TABLE %s RENAME TO %s;\n"
          + "COMMIT;", hologresConfigs.table, suffix, hologresConfigs.tempTableForOverwrite, onlyTablename)
      } else {
        sql = String.format("-- From Spark-Connector: <replace the original table with the temp table when overwrite success>\n"
          + "set hg_experimental_force_sync_replay=on;\n"
          + "BEGIN;\n"
          + "DROP TABLE IF EXISTS %s %s;\n"
          + "ALTER TABLE %s RENAME TO %s;\n"
          + "ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN(\'%s\');\n"
          + "COMMIT;", hologresConfigs.table, suffix, hologresConfigs.tempTableForOverwrite, onlyTablename,
          parentTable, hologresConfigs.table, partitionValue)
      }
      logger.info(s"rename temp table for overwrite DDL: \n$sql")
      statement.execute(sql)
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * delete temp table when overwrite failed
   */
  def deleteTempTableForOverWrite(hologresConfigs: HologresConfigs): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)
    val suffix = getDropTableForceSuffix(hologresConfigs)
    /*
    BEGIN ;
    -- 删除临时表
    DROP TABLE IF EXISTS <table>;
    COMMIT ;
    */
    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      val statement = conn.createStatement()
      val sql = String.format("-- From Spark-Connector: <delete temp table when overwrite failed>\n"
        + "set hg_experimental_force_sync_replay=on;\n"
        + "BEGIN;\n"
        + "DROP TABLE IF EXISTS %s %s;\n"
        + "COMMIT;", hologresConfigs.tempTableForOverwrite, suffix)
      logger.info(s"drop temp table for overwrite DDL: \n$sql")
      statement.execute(sql)
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  def createStage(hologresConfigs: HologresConfigs, stage: String) : Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)

    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      if (hologresConfigs.copyStageOnly) {
        JDBCUtil.executeSql(conn, s"set hg_experimental_internal_stage_max_ttl_in_seconds=${hologresConfigs.copyStageTtl}", ignoreException = true)
        JDBCUtil.executeSql(conn, s"call hologres.hg_create_internal_stage('$stage', '${hologresConfigs.table}', ${hologresConfigs.copyStageTtl})")
      } else {
        JDBCUtil.executeSql(conn, s"call hologres.hg_create_internal_stage('$stage', '${hologresConfigs.table}', 7200)")
      }
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  def dropStages(hologresConfigs: HologresConfigs, stages : Array[String]): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)

    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      for (stage <- stages) {
        JDBCUtil.executeSql(conn,"call hologres.hg_drop_internal_stage('" + stage + "');")
      }
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  def insertFromStages(hologresConfigs: HologresConfigs,
                       schema: TableSchema,
                       columnNames: util.List[String],
                       stages: util.List[String],
                       conflictAction: OnConflictAction,
                       isOverwrite: Boolean): Unit = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)

    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs, database = null , simple = true)
      JDBCUtil.executeSql(conn, "set hg_experimental_affect_row_multiple_times_keep_last = on")
      if (hologresConfigs.enableServerlessComputing) {
        JDBCUtil.executeSql(conn, "set hg_computing_resource = 'serverless'")
        JDBCUtil.executeSql(conn, s"set hg_experimental_serverless_tasks_required_cores = ${hologresConfigs.serverlessComputingRequiredCores}")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_query_priority = ${hologresConfigs.serverlessComputingQueryPriority}")
      }
      if (!isOverwrite) {
        JDBCUtil.executeSql(conn, CopyUtil.buildInsertTableSelectFromStageSql(schema, columnNames, stages, conflictAction))
      } else {
        JDBCUtil.executeSql(conn, CopyUtil.buildInsertOverwriteTableSelectFromStageSql(schema, columnNames, stages))
      }
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  def getChildTablePartitionInfo(hologresConfigs: HologresConfigs): (String, String) = {
    logger.setSparkAppName(hologresConfigs.sparkAppName)
    logger.setSparkAppId(hologresConfigs.sparkAppId)
    logger.setHoloTableName(hologresConfigs.table)
    /*
    -- 获取父表名称(test_table)和当前子表的分区值(20230527)
    CREATE TABLE public.test_table_20230527 PARTITION OF test_table
      FOR VALUES IN ('20230527');
    */
    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      val statement = conn.createStatement()
      val rs: ResultSet = statement.executeQuery(String.format("select hg_dump_script('%s');", hologresConfigs.table))
      if (rs.next) {
        val pattern = "PARTITION OF ([^']*)\n  FOR VALUES IN \\('([^']*)'\\);".r
        val dumpScript = rs.getString(1)
        logger.info(s"got dump script : \n$dumpScript")
        val matchOption = pattern.findFirstMatchIn(dumpScript)
        matchOption match {
          case Some(m) => return (m.group(1), m.group(2))
          case _ =>
        }
      }
      null
    } catch {
      case e: SQLException =>
        throw new RuntimeException(e)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * 根据用户传入的表名或者view名,查询其依赖的底层表
   * */
  private def getBaseTables(connection: Connection, tableName: TableName): Array[(String, String, String)] = {
    val statement = connection.createStatement()
    val rs: ResultSet = statement.executeQuery(String.format(
      """
WITH RECURSIVE start_rel AS (
  -- 起点：输入对象，可以是 view/mview/table/foreign table
  SELECT
    c.oid,
    n.nspname AS schema_name,
    c.relname,
    c.relkind
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '%s'
    AND c.relname = '%s'              -- 输入对象名
    AND c.relkind IN ('v','m','r','f','p') -- 视图/物化视图/普通表/外表
),
view_tree AS (
  -- 只把起点里的 view/mview 放进递归
  SELECT
    c1.oid        AS view_oid,
    ARRAY[c1.oid] AS path
  FROM start_rel s
  JOIN pg_class c1 ON c1.oid = s.oid
  WHERE c1.relkind IN ('v','m')

  UNION ALL

  -- 递归：从当前 view_oid 的 rewrite 规则中找依赖对象
  SELECT
    c2.oid            AS view_oid,
    path || c2.oid    AS path
  FROM view_tree vt
  JOIN pg_rewrite rw   ON rw.ev_class = vt.view_oid
  JOIN pg_depend  d    ON d.objid     = rw.oid
  JOIN pg_class   c2   ON d.refobjid  = c2.oid
  WHERE
    c2.relkind IN ('v','m')           -- 只对视图/物化视图继续递归
    AND NOT (c2.oid = ANY(vt.path))   -- 防环：避免重复访问同一个 view
),
-- 从视图递归树中找到底层表/外表
view_base AS (
  SELECT DISTINCT
    n2.nspname AS schema_name,
    c2.relname AS rel_name,
    c2.relkind
  FROM view_tree vt
  JOIN pg_rewrite rw   ON rw.ev_class = vt.view_oid
  JOIN pg_depend  d    ON d.objid     = rw.oid
  JOIN pg_class   c2   ON d.refobjid  = c2.oid
  JOIN pg_namespace n2 ON n2.oid      = c2.relnamespace
  WHERE c2.relkind IN ('r','f','p')       -- 普通表 + 外表
),
-- 起点本身如果就是表/外表，也把它算入结果
self_if_base AS (
  SELECT
    s.schema_name,
    s.relname AS rel_name,
    s.relkind
  FROM start_rel s
  WHERE s.relkind IN ('r','f','p')
)

-- 最终结果：递归展开得到的底层表 ∪ 起点是表/外表时的自身
SELECT DISTINCT
  schema_name,
  rel_name,
  relkind
FROM (
  SELECT * FROM view_base
  UNION ALL
  SELECT * FROM self_if_base
) t;
      """, tableName.getSchemaName, tableName.getTableName))
    val result = new ArrayBuffer[(String, String, String)]()
    while (rs.next) {
      result.append((rs.getString(1), rs.getString(2), rs.getString(3)))
    }
    result.toArray
  }

  def getTableGroupName(connection: Connection, tableName: TableName): String = {
    val statement = connection.createStatement()
    val rs: ResultSet = statement.executeQuery(String.format(
      """
         select property_value from hologres.hg_table_properties where table_namespace = '%s' and table_name = '%s' and property_key= 'table_group' ;
      """, tableName.getSchemaName, tableName.getTableName))
    if (rs.next) {
      return rs.getString(1)
    }
    null
  }

  /**
   * 要求所有的表都是分区表,且分区键相同
   */
  def getPartitionColumnAndValues(hologresConfigs: HologresConfigs): (String, Array[String]) = {

    var firstTableName: TableName = null
    var holoPartitionColumn: String = null
    val holoPartitionValues: java.util.Set[String] = new java.util.HashSet[String]()

    @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      val baseTables = holoClient.sql[Array[(String, String, String)]](
        (conn: Connection) => getBaseTables(conn, TableName.valueOf(hologresConfigs.table))
      ).get()


      for ((schema, table, kind) <- baseTables) {
        val tableName: TableName = TableName.valueOf(IdentifierUtil.quoteIdentifier(schema, true), IdentifierUtil.quoteIdentifier(table, true))
        if (!kind.equals("p")) {
          throw new IllegalArgumentException(s"table ${tableName.getFullName} is not a partitioned table")
        }
        if (firstTableName == null) {
          firstTableName = tableName
        }
        val partitionColumnName = holoClient.sql[String]((conn: Connection) => ConnectionUtil.getPartitionColumnName(conn, tableName)).get()
        if (partitionColumnName == null) {
          throw new IllegalArgumentException(s"table ${tableName.getFullName} is not a partitioned table")
        }
        if (holoPartitionColumn == null) {
          holoPartitionColumn = partitionColumnName
        } else {
          if (!holoPartitionColumn.equals(partitionColumnName)) {
            throw new IllegalArgumentException(s"table ${tableName.getFullName} and ${firstTableName.getFullName} has different partition column")
          }
        }
        val partitionValues = Command.getPartValueToPartitionTableMap(holoClient, (new TableSchema.Builder).setTableName(tableName).build()).keySet()
        holoPartitionValues.addAll(partitionValues)
      }
    } finally {
      holoClient.close()
    }
    (holoPartitionColumn, holoPartitionValues.toArray(Array.empty[String]))
  }

  /**
   * 要求所有的表shard count相同
   */
  def getShardCount(hologresConfigs: HologresConfigs): Int = {
    var shardCount: Int = 1
    var firstTableName: TableName = null

    @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      val baseTables = holoClient.sql[Array[(String, String, String)]](
        (conn: Connection) => getBaseTables(conn, TableName.valueOf(hologresConfigs.table))
      ).get()

      for ((schema, table, kind) <- baseTables) {
        val tableName: TableName = TableName.valueOf(IdentifierUtil.quoteIdentifier(schema, true), IdentifierUtil.quoteIdentifier(table, true))
        if (firstTableName == null) {
          firstTableName = tableName
        }
        // 只有内表才存在分片
        if (!kind.equals("r") && !kind.equals("p")) {
          throw new IllegalArgumentException(s"table ${tableName.getFullName} is not a holo table")
        }
        val holoSchema = holoClient.getTableSchema(tableName)

        if (shardCount == 1) {
          shardCount = Command.getShardCount(holoClient, holoSchema)
        } else {
          if (shardCount != Command.getShardCount(holoClient, holoSchema)) {
            throw new IllegalArgumentException(s"table ${tableName.getFullName} and ${firstTableName.getFullName} has different shard count")
          }
        }
      }
    } finally {
      holoClient.close()
    }
    shardCount
  }

}
