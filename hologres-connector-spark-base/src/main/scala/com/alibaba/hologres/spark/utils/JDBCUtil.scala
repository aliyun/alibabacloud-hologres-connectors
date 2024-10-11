package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.function.FunctionWithSQLException
import com.alibaba.hologres.client.impl.util.ConnectionUtil
import com.alibaba.hologres.client.model.{HoloVersion, TableName}
import com.alibaba.hologres.org.postgresql.PGProperty
import com.alibaba.hologres.spark.config.HologresConfigs
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import com.alibaba.hologres.client.utils.IdentifierUtil
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.{Objects, Properties}

/** JDBC utils. */
object JDBCUtil {
  private val logger = LoggerFactory.getLogger(getClass)

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

  def couldDirectConnect(configs: HologresConfigs): Boolean = {
    val url = configs.jdbcUrl
    val info = new Properties
    PGProperty.USER.set(info, configs.username)
    PGProperty.PASSWORD.set(info, configs.password)
    PGProperty.APPLICATION_NAME.set(info, "hologres-connector-spark_copy")
    val directUrl = getJdbcDirectConnectionUrl(configs)
    var conn: Connection = null
    logger.info("try connect directly to holo with url {}", url)
    try {
      conn = DriverManager.getConnection(directUrl, info)
    } catch {
      case _: Exception =>
        logger.warn("could not connect directly to holo.")
        return false
    } finally if (conn != null) conn.close()
    true
  }

  // Returns the jdbc url directly connected to fe
  def getJdbcDirectConnectionUrl(configs: HologresConfigs): String = {
    var endpoint: String = null
    try Class.forName("com.alibaba.hologres.org.postgresql.Driver")
    catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
    val conn = DriverManager.getConnection(configs.jdbcUrl, configs.username, configs.password)
    try {
      val stat = conn.createStatement
      try {
        val rs = stat.executeQuery("select inet_server_addr(), inet_server_port()")
        try {
          if (rs.next) {
            endpoint = rs.getString(1) + ":" + rs.getString(2)
          }
          if (Objects.isNull(endpoint)) {
            throw new RuntimeException("Failed to query \"select inet_server_addr(), inet_server_port()\".")
          }
        } finally if (rs != null) rs.close()
      }
      finally if (stat != null) stat.close()
    }
    catch {
      case t: SQLException =>
        throw new RuntimeException(t)
    } finally if (conn != null) conn.close()
    replaceJdbcUrlEndpoint(configs.jdbcUrl, endpoint)
  }

  private def replaceJdbcUrlEndpoint(originalUrl: String, newEndpoint: String) = {
    val replacement = "//" + newEndpoint + "/"
    originalUrl.replaceFirst("//\\S+/", replacement)
  }

  object getHoloVersion extends FunctionWithSQLException[Connection, HoloVersion] {
    override def apply(conn: Connection): HoloVersion = {
      ConnectionUtil.getHoloVersion(conn)
    }
  }

  def getSimpleSelectFromStatement(table: String, selectFields: Array[String]): String = {
    val selectExpressions: String = selectFields.mkString(", ")
    "SELECT " + selectExpressions + " FROM " + table
  }

  def createConnection(hologresConfigs: HologresConfigs): Connection = {
    try Class.forName("com.alibaba.hologres.org.postgresql.Driver")
    catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
    try {
      val info: Properties = new Properties
      PGProperty.USER.set(info, hologresConfigs.username)
      PGProperty.PASSWORD.set(info, hologresConfigs.password)
      PGProperty.APPLICATION_NAME.set(info, "spark_connector_util")
      val conn = DriverManager.getConnection(hologresConfigs.jdbcUrl, info)
      logger.info("create connection to holo with url {}", hologresConfigs.jdbcUrl)
      conn
    } catch {
      case e: SQLException =>
        throw new RuntimeException(String.format("Failed getting connection to %s because %s", hologresConfigs.jdbcUrl, ExceptionUtils.getStackTrace(e)))
    }
  }

  def generateTempTableNameForOverwrite(hologresConfigs: HologresConfigs): String = {
    val tableName: TableName = TableName.valueOf(hologresConfigs.table)
    val tempTableName = String.format("tmp_spark_to_holo_overwrite_%s_%s_%s"
      , System.currentTimeMillis.toString
      , hologresConfigs.sparkAppName
      , tableName.getTableName)

    TableName.valueOf(
      IdentifierUtil.quoteIdentifier(tableName.getSchemaName, true),
      // holo表名长度限制127个字符
      IdentifierUtil.quoteIdentifier(tempTableName.substring(0, math.min(tempTableName.length, 127)), true)
    ).getFullName
  }

  def createTempTableForOverWrite(hologresConfigs: HologresConfigs): Unit = {
    /*
    BEGIN ;
    -- 清理潜在的临时表
    DROP TABLE IF EXISTS <table_new>;
    -- 创建临时表
    SET hg_experimental_enable_create_table_like_properties=on;
    CALL HG_CREATE_TABLE_LIKE ('<table_new>', 'select * from <table>');
    COMMIT ;
    */
    var conn: Connection = null
    try {
      conn = createConnection(hologresConfigs)
      val statement = conn.createStatement()
      val sql = String.format("BEGIN;\n"
        + "DROP TABLE IF EXISTS %s;\n"
        + "set hg_experimental_enable_create_table_like_properties=on;\n"
        + "CALL HG_CREATE_TABLE_LIKE ('%s', 'select * from %s');\n"
        + "COMMIT;", hologresConfigs.tempTableForOverwrite, hologresConfigs.tempTableForOverwrite, hologresConfigs.table)
      logger.info("create temp table for overwrite DDL: \n{}", sql)
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

  def renameTempTableForOverWrite(hologresConfigs: HologresConfigs, parentTable: String = null, partitionValue: String = null): Unit = {
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
        sql = String.format("BEGIN;\n"
          + "DROP TABLE IF EXISTS %s;\n"
          + "ALTER TABLE %s RENAME TO %s;\n"
          + "COMMIT;", hologresConfigs.table, hologresConfigs.tempTableForOverwrite, onlyTablename)
      } else {
        sql = String.format("BEGIN;\n"
          + "DROP TABLE IF EXISTS %s;\n"
          + "ALTER TABLE %s RENAME TO %s;\n"
          + "ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN(\'%s\');\n"
          + "COMMIT;", hologresConfigs.table, hologresConfigs.tempTableForOverwrite, onlyTablename,
          parentTable, hologresConfigs.table, partitionValue)
      }
      logger.info("rename temp table for overwrite DDL: \n{}", sql)
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

  def deleteTempTableForOverWrite(hologresConfigs: HologresConfigs): Unit = {
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
      val sql = String.format("BEGIN;\n"
        + "DROP TABLE IF EXISTS %s;\n"
        + "COMMIT;", hologresConfigs.tempTableForOverwrite)
      logger.info("drop temp table for overwrite DDL: \n{}", sql)
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

  def getChildTablePartitionInfo(hologresConfigs: HologresConfigs): (String, String) = {
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
        logger.info("got dump script : \n{}", dumpScript)
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

}
