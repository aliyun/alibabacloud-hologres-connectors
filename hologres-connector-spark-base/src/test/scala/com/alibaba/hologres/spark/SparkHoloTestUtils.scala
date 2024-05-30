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

object RowShardUtil {
  // hologres仅支持int等整数类型,字符串类型,Date类型做distribution key,大多数类型toString的字面值与holo是一致的,因此不存在计算错误的情况
  // 但date类型在holo中存储为int,如果遇到Date类型,则转换为int计算
  // 类似的,目前timestamptz类型也可以当作distribution key(timestamp类型不可以),因此计算shard使用其对应的long值.
  def hash(row: Row, indexes: Array[Int]): Int = {
    var hash = 0
    var first = true
    if (indexes == null || indexes.length == 0) {
      val rand = ThreadLocalRandom.current
      hash = rand.nextInt
    }
    else for (i <- indexes) {
      var o = row.get(i)
      o match {
        case date: Date => o = date.toLocalDate.toEpochDay
        case timestamp: Timestamp => o = TimestampUtil.timestampToMillisecond(timestamp, "timestamptz")
        case _ =>
      }
      if (first) hash = ShardUtil.hash(o)
      else hash ^= ShardUtil.hash(o)
      first = false
    }
    hash
  }
}

class HoloKeySelector(shardCount: Int, sparkSchema: StructType, holoSchema: TableSchema) extends Serializable {
  final private val splitRange = new ConcurrentSkipListMap[Integer, Integer]
  val range: Array[Array[Int]] = ShardUtil.split(shardCount)
  for (i <- 0 until shardCount) {
    splitRange.put(range(i)(0), i)
  }

  val keyIndexInHolo: Array[Int] = holoSchema.getDistributionKeyIndex
  val keyIndexInSpark = new Array[Int](keyIndexInHolo.length)
  for (i <- keyIndexInHolo.indices) {
    val columnName = holoSchema.getColumn(keyIndexInHolo(i)).getName
    try {
      keyIndexInSpark(i) = sparkSchema.fieldIndex(columnName)
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(String.format("repartition need write all distribution keys %s, missing column info: %s",
          holoSchema.getDistributionKeys, e.getMessage))
    }
  }

  /**
   * 根据Row中的distribution key字段的信息，计算写入holo时所处的shard
   */
  def getKey(value: Row): Integer = {
    val raw = RowShardUtil.hash(value, keyIndexInSpark)
    val hash = Integer.remainderUnsigned(raw, ShardUtil.RANGE_END)
    splitRange.floorEntry(hash).getValue
  }
}

class CustomerPartition(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    Integer.valueOf(key.toString) % numPartitions
  }
}
