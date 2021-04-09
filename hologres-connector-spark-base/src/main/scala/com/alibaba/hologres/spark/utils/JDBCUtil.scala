package com.alibaba.hologres.spark.utils

import org.apache.commons.cli.MissingArgumentException
import org.slf4j.LoggerFactory

/** JDBC utils. */
object JDBCUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  def getDbUrl(endpoint: String, database: String): String = {
    if (!checkEndpoint(endpoint)) {
      throw new MissingArgumentException("Format error of parameter 'endpoint'.")
    }

    "jdbc:postgresql://" + endpoint + "/" + database
  }

  // end with Port and not start with ':'
  def checkEndpoint(endpoint: String): Boolean = {
    val portPatten = ("(.+):(([1-9]([0-9]{0,4}))|([1-6][0-5][0-5][0-3][0-5]))$").r

    1 == portPatten.findAllIn(endpoint).size
  }
}
