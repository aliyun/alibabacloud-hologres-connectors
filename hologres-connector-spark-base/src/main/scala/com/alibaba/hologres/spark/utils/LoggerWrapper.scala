package com.alibaba.hologres.spark.utils

import org.slf4j.{Logger, LoggerFactory}

class LoggerWrapper(clazz: Class[_]) {
  val logger: Logger = LoggerFactory.getLogger(clazz)

  private var sparkAppName: String = ""
  private var sparkAppId: String = ""
  private var sparkTaskId: String = ""
  private var holoTableName: String = ""

  def setSparkAppName(appName: String): Unit = {
    sparkAppName = appName
  }

  def setSparkAppId(appId: String): Unit = {
    sparkAppId = appId
  }

  def setSparkTaskId(taskId: String): Unit = {
    sparkTaskId = taskId
  }

  def setHoloTableName(tableName: String): Unit = {
    holoTableName = tableName
  }

  private def getLogPreFix: String = {
    var prefix = ""
    if (sparkAppName != null && sparkAppName.nonEmpty) {
      prefix += sparkAppName
    }
    if (sparkAppId != null && sparkAppId.nonEmpty) {
      prefix += s"-${sparkAppId}"
    }
    if (sparkTaskId != null && sparkTaskId.nonEmpty) {
      prefix += s"-${sparkTaskId}"
    }
    if (holoTableName != null && holoTableName.nonEmpty) {
      prefix += s"-${holoTableName}"
    }
    if (prefix.nonEmpty) {
      prefix = s"[$prefix] "
    }
    prefix
  }

  def info(msg: String): Unit = {
    logger.info(s"${getLogPreFix}${msg}")
  }

  def error(msg: String): Unit = {
    logger.error(s"${getLogPreFix}${msg}")
  }

  def error(msg: String, t: Throwable): Unit = {
    logger.error(s"${getLogPreFix}${msg}", t)
  }

  def debug(msg: String): Unit = {
    logger.debug(s"${getLogPreFix}${msg}")
  }

  def warn(msg: String): Unit = {
    logger.warn(s"${getLogPreFix}${msg}")
  }

  def warn(msg: String, t: Throwable): Unit = {
    logger.warn(s"${getLogPreFix}${msg}", t)
  }

}
