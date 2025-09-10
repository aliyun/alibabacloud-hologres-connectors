/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.spark.source.copy

import com.alibaba.hologres.client.copy.out.arrow.ArrowReader
import com.alibaba.hologres.client.impl.util.ConnectionUtil
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.org.postgresql.PGProperty
import com.alibaba.hologres.org.postgresql.copy.CopyManager
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.{JDBCUtil, LoggerWrapper}

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

class CopyContext {
  private val logger = new LoggerWrapper(getClass)

  var pgConn: PgConnection = _
  var manager: CopyManager = _
  var arrowReader: ArrowReader = _
  var schema: TableSchema = _

  def init(configs: HologresConfigs): Unit = {
    logger.setSparkAppName(configs.sparkAppName)
    logger.setSparkAppId(configs.sparkAppId)
    logger.setHoloTableName(configs.table)
    try Class.forName("com.alibaba.hologres.org.postgresql.Driver")
    catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(e)
    }
    var conn: Connection = null
    var url = configs.jdbcUrl

    val info: Properties = new Properties
    PGProperty.USER.set(info, configs.username)
    PGProperty.PASSWORD.set(info, configs.password)
    PGProperty.SOCKET_TIMEOUT.set(info, 360)
    PGProperty.APPLICATION_NAME.set(info, configs.holoConfig.getAppName + "_copy")
    if (configs.enableAkv4) {
      JDBCUtil.setAkv4Region(info, configs.akv4Region)
    }

    try {
      if (configs.directConnect) {
        url = ConnectionUtil.getDirectConnectionUrl(url, info, false)
      }
      if (conn == null) {
        logger.info("init conn success to " + url)
        conn = DriverManager.getConnection(url, info)
      }

      JDBCUtil.executeSql(conn, s"set statement_timeout = '${configs.statementTimeout}s'")
      // server less computing
      if (configs.enableServerlessComputing) {
        JDBCUtil.executeSql(conn, "set hg_computing_resource = 'serverless'")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_query_priority = ${configs.serverlessComputingQueryPriority}")
        JDBCUtil.executeSql(conn, s"SET hg_experimental_serverless_computing_required_cores = 5")
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
