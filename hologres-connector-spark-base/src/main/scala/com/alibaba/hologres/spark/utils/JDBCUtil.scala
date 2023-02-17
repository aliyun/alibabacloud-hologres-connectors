package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.function.FunctionWithSQLException
import com.alibaba.hologres.client.impl.util.ConnectionUtil
import com.alibaba.hologres.client.model.HoloVersion
import com.alibaba.hologres.org.postgresql.PGProperty
import com.alibaba.hologres.spark.config.HologresConfigs
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, SQLException}
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
    try {
      var conn: Connection = null
      logger.info("try connect directly to holo with url {}", url)
      try {
        conn = DriverManager.getConnection(directUrl, info)
      } catch {
        case _: Exception =>
          logger.warn("could not connect directly to holo.")
          return false
      } finally if (conn != null) conn.close()
    }
    true
  }

  // Returns the jdbc url directly connected to fe
  def getJdbcDirectConnectionUrl(configs: HologresConfigs): String = {
    var endpoint: String = null
    try {
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
    }
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
}
