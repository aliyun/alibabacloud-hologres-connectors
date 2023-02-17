package com.alibaba.hologres.spark.sink.copy

import com.alibaba.hologres.client.copy.RecordOutputStream
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

  def init(configs: HologresConfigs): Unit = {
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
      if (configs.copy_write_direct_connect) {
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
