package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.{Column, HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.io.IOException
import java.sql.Types
import scala.collection.mutable.ArrayBuffer

object SparkHoloUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  // 检查schema是否匹配, 其实是根据holo表或者查询的query生成一个默认的spark schema, 和传入的spark schema进行比较,
  // 传入的spark schema必须是holo表或者查询的query生成的spark schema的一部分
  def checkSparkTableSchema(hologresConfigs: HologresConfigs, sparkSchema: StructType, mockHoloSchemaForQuery: TableSchema = null): Unit = {
    var holoSchema: TableSchema = null
    if (hologresConfigs.isTableSource && holoSchema == null) {
      @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
      try {
        holoSchema = holoClient.getTableSchema(hologresConfigs.table)
      } finally {
        holoClient.close()
      }
    } else {
      holoSchema = mockHoloSchemaForQuery
    }
    logger.info("spark schema: " + sparkSchema.toDDL)
    logger.info("holo schema: " + holoSchema)
    sparkSchema.fields.foreach(column => {
      if (holoSchema.getColumnIndex(column.name) == null) {
        throw new IllegalArgumentException(String.format("column %s does not exist in hologres table %s", column.name, holoSchema.getTableName))
      }
      val holoColumn = holoSchema.getColumn(holoSchema.getColumnIndex(column.name))
      if (column.dataType != getSparkDataType(holoColumn)) {
        throw new IllegalArgumentException(String.format("column %s in hologres table %s type does not match: spark type: %s, hologres type: %s",
          column.name, holoSchema.getTableName, column.dataType, holoColumn.getTypeName))
      }
    })
  }

  // 如果未传入spark的DDL，则根据holo表或者查询的query推断一个默认的spark schema
  def inferSparkTableSchema(hologresConfigs: HologresConfigs, mockHoloSchemaForQuery: TableSchema = null): StructType = {
    var holoSchema: TableSchema = null
    if (hologresConfigs.isTableSource && holoSchema == null) {
      @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
      try {
        holoSchema = holoClient.getTableSchema(hologresConfigs.table)
      } finally {
        holoClient.close()
      }
    } else {
      holoSchema = mockHoloSchemaForQuery
    }
    val fields = ArrayBuffer[StructField]()
    holoSchema.getColumnSchema.foreach(column => {
      fields += StructField(column.getName, getSparkDataType(column))
    })

    StructType(fields)
  }

  def mockHoloSchemaForQuery(hologresConfigs: HologresConfigs): TableSchema = {
    @transient val conn = JDBCUtil.createConnection(hologresConfigs)
    try {
      var holoSchema: TableSchema = null
      val metadata = conn.prepareStatement(hologresConfigs.query).getMetaData
      val columnCount = metadata.getColumnCount
      // 如果通过query查询holo,这里mock一个holo schema
      val mockSchemaBuilder: TableSchema.Builder = new TableSchema.Builder()
      for (i <- 1 to columnCount) {
        val column: Column = new Column()
        column.setName(metadata.getColumnName(i))
        column.setType(metadata.getColumnType(i))
        column.setTypeName(metadata.getColumnTypeName(i))
        column.setPrecision(metadata.getPrecision(i))
        column.setScale(metadata.getScale(i))
        column.setArrayType(column.getTypeName.startsWith("_"))
        if (column.isArrayType) {
          column.setArrayElementType(column.getTypeName.replaceFirst("_", ""))
        }
        mockSchemaBuilder.addColumn(column)
      }
      mockSchemaBuilder.setTableName(TableName.quoteValueOf("", s"mock_table_from_query(${hologresConfigs.query})"))
      holoSchema = mockSchemaBuilder.build()
      holoSchema.calculateProperties()
      holoSchema
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(String.format("could not get metadata for query [%s]", hologresConfigs.query), e)
    } finally {
      conn.close()
    }
  }

  def getSparkDataType(column: Column): DataType = {
    column.getType match {
      case Types.SMALLINT | Types.TINYINT => DataTypes.ShortType
      case Types.INTEGER => DataTypes.IntegerType
      case Types.BIGINT => DataTypes.LongType
      case Types.REAL | Types.FLOAT => DataTypes.FloatType
      case Types.DOUBLE => DataTypes.DoubleType
      case Types.BOOLEAN | Types.BIT => DataTypes.BooleanType
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => DataTypes.TimestampType
      case Types.BINARY => DataTypes.BinaryType
      case Types.DATE => DataTypes.DateType
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR => DataTypes.StringType
      case Types.NUMERIC | Types.DECIMAL => DecimalType(column.getPrecision, column.getScale)
      case Types.ARRAY =>
        column.getTypeName match {
          case "_int4" => ArrayType(DataTypes.IntegerType)
          case "_int8" => ArrayType(DataTypes.LongType)
          case "_float4" => ArrayType(DataTypes.FloatType)
          case "_float8" => ArrayType(DataTypes.DoubleType)
          case "_bool" => ArrayType(DataTypes.BooleanType)
          case "_text" => ArrayType(DataTypes.StringType)
        }
      case _ =>
        column.getTypeName match {
          case "roaringbitmap" => DataTypes.BinaryType
          case "json" | "jsonb" => DataTypes.StringType
          case _ => throw new IllegalArgumentException(String.format("Column type %s does not supported now",
            column.getTypeName))
        }
    }
  }

  def chooseBestMode(sparkSchema: StructType, hologresConfigs: HologresConfigs): HologresConfigs = {
    val holoClient: HoloClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      val holoSchema = holoClient.getTableSchema(TableName.valueOf(hologresConfigs.table))
      var holoVersion: HoloVersion = null
      try holoVersion = holoClient.sql[HoloVersion](getHoloVersion).get()
      catch {
        case e: Exception =>
          throw new IOException("Failed to get holo version", e)
      }

      // 2.2.25之后支持全字段时的bulk_load_onc_conflict, 3.1.0之后支持部分字段的bulk_load_on_conflict
      val supportBulkLoadOnConflict = holoSchema.getPrimaryKeys.length > 0 &&
        ((holoVersion.compareTo(new HoloVersion(2, 2, 25)) > 0 && sparkSchema.fields.length == holoSchema.getColumnSchema.length)
          || holoVersion.compareTo(new HoloVersion(3, 1, 0)) > 0)
      val supportBulkLoad = holoSchema.getPrimaryKeys.length == 0 && holoVersion.compareTo(new HoloVersion(2, 1, 0)) > 0
      val supportStreamCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0
      val couldReshuffle = holoSchema.getDistributionKeys.length > 0
      // choose best write mode
      if ("auto" == hologresConfigs.writeMode) {
        if (supportBulkLoadOnConflict) {
          hologresConfigs.writeMode = CopyMode.BULK_LOAD_ON_CONFLICT
          // 数据未经过reshuffle, 则将needReshuffle设置为true
          if (!hologresConfigs.reshuffleByHoloDistributionKey && !hologresConfigs.needReshuffle && couldReshuffle) {
            hologresConfigs.needReshuffle = true
          }
        } else if (supportBulkLoad) {
          hologresConfigs.writeMode = CopyMode.BULK_LOAD
          // 无主键表如果配置了distribution key, 也可以reshuffle
          if (!hologresConfigs.reshuffleByHoloDistributionKey && !hologresConfigs.needReshuffle && couldReshuffle) {
            hologresConfigs.needReshuffle = true
          }
        } else if (supportStreamCopy) {
          hologresConfigs.writeMode = CopyMode.STREAM
        } else {
          hologresConfigs.writeMode = "insert"
        }
        logger.info(s"choose best write mode: ${hologresConfigs.writeMode}")
        if (hologresConfigs.needReshuffle) {
          logger.info(s"need reshuffle.")
        }
      }

      // choose best read mode
      if ("auto" == hologresConfigs.readMode) {
        val supportCompressed = holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0
        var hasJsonBType = false
        sparkSchema.fields.foreach(column => {
          if (holoSchema.getColumnIndex(column.name) == null) {
            throw new IllegalArgumentException(String.format("column %s does not exist in hologres table %s", column.name, holoSchema.getTableName))
          }
          val holoColumn = holoSchema.getColumn(holoSchema.getColumnIndex(column.name))
          if (holoColumn.getTypeName == "jsonb") {
            hasJsonBType = true
          }
        })
        if (hasJsonBType) {
          hologresConfigs.readMode = "select"
        } else if (supportCompressed) {
          hologresConfigs.readMode = "bulk_read_compressed"
        } else {
          hologresConfigs.readMode = "bulk_read"
        }
        logger.info(s"choose best read mode: ${hologresConfigs.readMode}")
      }
      // 尝试直连，无法直连则各个tasks内不需要进行尝试
      if (hologresConfigs.directConnect) {
        hologresConfigs.directConnect = JDBCUtil.couldDirectConnect(hologresConfigs)
      }
      hologresConfigs
    } finally {
      if (holoClient != null) {
        holoClient.close()
      }
    }
  }
}
