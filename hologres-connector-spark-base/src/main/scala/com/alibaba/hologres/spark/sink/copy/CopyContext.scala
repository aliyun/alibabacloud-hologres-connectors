package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.copy.in.RecordOutputStream
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.org.postgresql.PGProperty
import com.alibaba.hologres.org.postgresql.copy.CopyManager
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

class CopyContext {
  private val logger = LoggerFactory.getLogger(getClass)

  var pgConn: PgConnection = _
  var manager: CopyManager = _
  var os: RecordOutputStream = _
  var schema: TableSchema = _

  def init(configs: HologresConfigs, targetShards: String = ""): Unit = {
    try Class.forName("com.alibaba.hologres.org.postgresql.Driver")
    catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
    var conn: Connection = null
    val url = configs.jdbcUrl

    val info: Properties = new Properties
    PGProperty.USER.set(info, configs.username)
    PGProperty.PASSWORD.set(info, configs.password)
    PGProperty.APPLICATION_NAME.set(info, configs.holoConfig.getAppName + "_copy")

    try {
      // copy write mode 的瓶颈往往是vip endpoint的网络吞吐，因此我们在可以直连holo fe的场景默认使用直连
      if (configs.directConnect) {
        val directUrl = JDBCUtil.getJdbcDirectConnectionUrl(configs)
        try {
          logger.info("try connect directly to holo with url {}", directUrl)
          conn = DriverManager.getConnection(directUrl, info)
          logger.info("init conn success with direct url {}", directUrl)
        } catch {
          case e: Exception =>
            logger.warn("could not connect directly to holo.")
        }
      }
      if (conn == null) {
        logger.info("init conn success to " + url)
        conn = DriverManager.getConnection(url, info)
      }

      // 不抛出异常: copy不需要返回影响行数所以默认关闭,但此guc仅部分版本支持,而且设置失败不影响程序运行
      JDBCUtil.executeSql(conn, "SET hg_experimental_enable_fixed_dispatcher_affected_rows = off", ignoreException = true)
      JDBCUtil.executeSql(conn, s"set statement_timeout = '${configs.statementTimeout}s'")
      // server less computing
      if (configs.enableServerlessComputing) {
        if (configs.writeMode == CopyMode.STREAM) {
          // stream mode 不支持serverless
          throw new RuntimeException("STREAM copyMode is not supported use serverless computing now.")
        }
        JDBCUtil.executeSql(conn, "set hg_computing_resource = 'serverless'")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_query_priority = ${configs.serverlessComputingQueryPriority}")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_required_cores = 5")
      }
      if (configs.reshuffleByHoloDistributionKey && targetShards != "") {
        JDBCUtil.executeSql(conn, s"set hg_experimental_target_shard_list = '$targetShards'")
      }
      if (configs.writeMode == CopyMode.BULK_LOAD_ON_CONFLICT) {
        JDBCUtil.executeSql(conn, "set hg_experimental_copy_enable_on_conflict = on;", ignoreException = true)
        JDBCUtil.executeSql(conn, "set hg_experimental_affect_row_multiple_times_keep_last = on;")
      }
      pgConn = conn.unwrap(classOf[PgConnection])
      logger.info("init unwrap conn success")
      manager = new CopyManager(pgConn)
      logger.info("init new manager success")
    } catch {
      case e: SQLException =>
        if (null != conn) try conn.close()
        catch {
          case ignored: SQLException =>

        }
        pgConn = null
        manager = null
        throw new RuntimeException(e)
    }
  }

  def close(): Unit = {
    manager = null
    if (pgConn != null) {
      try pgConn.close()
      catch {
        case e: SQLException =>
          throw new RuntimeException(e)
      }
      pgConn = null
    }
  }
}
