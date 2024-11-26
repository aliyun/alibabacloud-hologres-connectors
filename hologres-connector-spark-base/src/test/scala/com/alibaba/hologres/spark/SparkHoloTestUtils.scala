package com.alibaba.hologres.spark

import com.alibaba.hologres.client.impl.util.ShardUtil
import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.client.{HoloClient, HoloConfig}
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil
import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.{Date, DriverManager, SQLException, Timestamp}
import java.util.concurrent.{ConcurrentSkipListMap, ThreadLocalRandom}

/** SparkHoloTestUtils. */
class SparkHoloTestUtils {
  var client: HoloClient = _

  // Optional: 1.Setting parameters manually
  var username: String = _
  var password: String = _
  var jdbcUrl: String = _

  def init(): Unit = {
    val holoConfig = new HoloConfig
    holoConfig.setJdbcUrl(jdbcUrl)
    holoConfig.setUsername(username)
    holoConfig.setPassword(password)
    holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true)
    client = new HoloClient(holoConfig)
    client.setAsyncCommit(true)
  }

  def createTableSql(schema: StructType, table: String): String = {
    var createSql: String = "CREATE TABLE "
    createSql += (table + " (\n")
    schema.foreach(field => {
      field.dataType match {
        case ShortType => createSql += (field.name + " SMALLINT")
        case IntegerType => createSql += (field.name + " INT")
        case LongType => createSql += (field.name + " BIGINT")
        case FloatType => createSql += (field.name + " FLOAT4")
        case DoubleType => createSql += (field.name + " DOUBLE PRECISION")
        case BooleanType => createSql += (field.name + " BOOL")
        case StringType =>
          field.name match {
            case "json_column" => createSql += (field.name + " JSON")
            case "jsonb_column" => createSql += (field.name + " JSONB")
            case _ => createSql += (field.name + " TEXT")
          }
        case TimestampType => createSql += (field.name + " TIMESTAMPTZ")
        case BinaryType =>
          field.name match {
            case "rb_column" => createSql += (field.name + " ROARINGBITMAP")
            case _ => createSql += (field.name + " bytea")
          }
        case DateType => createSql += (field.name + " DATE")
        case decimalType: DecimalType =>
          createSql += (field.name + " NUMERIC(" + decimalType.precision + ", " + decimalType.scale + ")")
        case arrayType: ArrayType =>
          arrayType.elementType match {
            case IntegerType => createSql += (field.name + " int4[]")
            case LongType => createSql += (field.name + " int8[]")
            case FloatType => createSql += (field.name + " float4[]")
            case DoubleType => createSql += (field.name + " float8[]")
            case BooleanType => createSql += (field.name + " boolean[]")
            case StringType => createSql += (field.name + " text[]")
          }
      }
      if (!field.nullable) {
        createSql += " not null"
      }
      if (field != schema.fields.last) {
        createSql += ",\n"
      } else {
        createSql += "\n"
      }
    })
    createSql += ");"
    println(createSql)
    dropTable(table)
    createTable(createSql, table)
    table
  }

  @throws[SQLException]
  def createTable(createSql: String, tableName: String): Unit = {
    createTable(createSql, tableName, hasPk = false)
  }

  @throws[SQLException]
  def createTable(createSql: String, tableName: String, hasPk: Boolean): Unit = {
    try {
      var createSql1: String = createSql
      val connection = DriverManager.getConnection(jdbcUrl, username, password)
      val statement = connection.createStatement
      if (!hasPk) {
        createSql1 = createSql.replace("primary key", "")
      }
      try statement.executeUpdate(createSql1.replace("TABLE_NAME", tableName))

      finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't create table " + tableName + " because " + ex.getMessage)
      }
    }
  }

  @throws[SQLException]
  def createSchema(schemaName: String): Unit = {
    try {
      val connection = DriverManager.getConnection(jdbcUrl, username, password)
      val statement = connection.createStatement

      try statement.execute("create schema if not exists " + schemaName)
      finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't create schema " + schemaName + " because " + ex.getMessage)
      }
    }
  }

  @throws[SQLException]
  def createPartitionTable(createSql: String, parentTableName: String, tableName: String, partitionValue: String): Unit = {
    try {
      var createSql1: String = createSql
      val connection = DriverManager.getConnection(jdbcUrl, username, password)
      val statement = connection.createStatement

      try statement.executeUpdate(createSql1
        .replace("PARENT_TABLE_NAME", parentTableName)
        .replace("TABLE_NAME", tableName)
        .replace("PARTITION_VALUE", partitionValue)
      )
      finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't create table " + tableName + " because " + ex.getMessage)
      }
    }
  }

  @throws[SQLException]
  def dropTable(tableName: String): Unit = {
    try {
      val connection = DriverManager.getConnection(jdbcUrl, username, password)
      val statement = connection.createStatement
      try statement.executeUpdate("Drop table if exists " + tableName)
      finally {
        if (statement != null) statement.close()
        if (connection != null) connection.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't drop table " + tableName + ex)
      }
    }
  }
}
