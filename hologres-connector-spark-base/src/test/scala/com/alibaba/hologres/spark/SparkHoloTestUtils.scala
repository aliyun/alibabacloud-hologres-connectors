package com.alibaba.hologres.spark

import java.sql.{DriverManager, SQLException}

import com.alibaba.hologres.client.{HoloClient, HoloConfig}
import com.alibaba.hologres.spark.utils.DataTypeUtil._
import com.alibaba.hologres.spark.utils.JDBCUtil._
import org.apache.spark.sql.types._

/** SparkHoloTestUtils. */
class SparkHoloTestUtils {
  var client: HoloClient = _

  // Optional: 1.Setting parameters manually
  var database: String = "test_database"
  var username: String = "your_username"
  var password: String = "your_password"
  var endpoint: String = "Ip:Port"
  var jdbcUrl: String = "jdbc:postgresql://Ip:Port/test_database"

  // Optional: 2.Get parameters from environment variables
//  val database: String = Option(System.getenv("HOLO_DATABASE_NAME")).getOrElse("")
//  val username: String = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
//  val password: String = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")
//  val region: String = Option(System.getenv("REGION_NAME")).getOrElse("cn-hangzhou")
//  val envType: String = Option(System.getenv("TEST_ENV_TYPE")).getOrElse("public").toLowerCase
//
//  if (envType != "private" && envType != "public") {
//    throw new Exception(
//      s"Unsupported test environment type: $envType, only support private or public")
//  }
//
//  val endpoint: String = Option(System.getenv("HOLO_ENDPOINT")).getOrElse {
//    envType match {
//      case "private" => s"http://dh-$region.aliyun-inc.com"
//      case "public" => s"https://dh-$region.aliyuncs.com"
//    }
//  }

  def init(): Unit = {
    val holoConfig = new HoloConfig
    val url = getDbUrl(endpoint, database)
    holoConfig.setJdbcUrl(url)
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
        case IntegerType => createSql += (field.name + " INT")
        case LongType => createSql += (field.name + " BIGINT primary key")
        case FloatType => createSql += (field.name + " FLOAT")
        case DoubleType => createSql += (field.name + " DOUBLE PRECISION")
        case BooleanType => createSql += (field.name + " BOOL")
        case StringType => createSql += (field.name + " TEXT")
        case TimestampType => createSql += (field.name + " TIMESTAMPTZ")
        case BinaryType => createSql += (field.name + " bytea")
        case DateType => createSql += (field.name + " DATE")
        case _: DecimalType => createSql += (field.name + " NUMERIC(38, 18)")
        case _: ArrayType =>
          getArrayType(field.dataType.toString) match {
            case "IntegerType" => createSql += (field.name + " int4[]")
            case "LongType" => createSql += (field.name + " int8[]")
            case "FloatType" => createSql += (field.name + " float4[]")
            case "DoubleType" => createSql += (field.name + " float8[]")
            case "BooleanType" => createSql += (field.name + " boolean[]")
            case "StringType" => createSql += (field.name + " text[]")
          }
      }
      if (field != schema.fields.last) {
        createSql += ",\n"
      } else {
        createSql += "\n"
      }
    })
    createSql += ");"
    //println(createSql)
    dropTable(table)
    createTable(createSql, table)
    table
  }

  @throws[SQLException]
  def createTable(createSql: String, tableName: String): Unit = {
    try {
      val connection = DriverManager.getConnection(getDbUrl(endpoint, database), username, password)
      val statement = connection.createStatement
      try statement.executeUpdate(createSql)
      finally {
        if (connection != null) connection.close()
        if (statement != null) statement.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't create table" + tableName + " because it already exists")
      }
    }
  }

  @throws[SQLException]
  def dropTable(tableName: String): Unit = {
    try {
      val connection = DriverManager.getConnection(getDbUrl(endpoint, database), username, password)
      val statement = connection.createStatement
      try statement.executeUpdate("Drop table " + tableName)
      finally {
        if (connection != null) connection.close()
        if (statement != null) statement.close()
      }
    } catch {
      case ex: SQLException => {
        println("Can't drop table " + tableName + " because it does not exist")
      }
    }
  }
}
