package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.client.copy.CopyMode
import com.alibaba.hologres.client.model.{Column, HoloVersion, TableName, TableSchema}
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.JDBCUtil.getHoloVersion
import org.apache.spark.sql.types._

import java.io.IOException
import java.sql.Types
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

object SparkHoloUtil {
  private val logger = new LoggerWrapper(getClass)

  // 检查schema是否匹配, 其实是根据holo表或者查询的query生成一个默认的spark schema, 和传入的spark schema进行比较,
  // 传入的spark schema必须是holo表或者查询的query生成的spark schema的一部分
  def checkSparkTableSchema(hologresConfigs: HologresConfigs, sparkSchema: StructType, holoSchema: TableSchema): Unit = {
    logger.info("spark schema: " + sparkSchema.toDDL)
    logger.info("holo schema: " + holoSchema)
    sparkSchema.fields.foreach(column => {
      if (holoSchema.getColumnIndex(column.name) == null) {
        throw new IllegalArgumentException(String.format("column %s does not exist in hologres table %s", column.name, holoSchema.getTableName))
      }
      val holoColumn = holoSchema.getColumn(holoSchema.getColumnIndex(column.name))
      if (!isSameDataType(column.dataType, getSparkDataType(holoColumn))) {
        if (!hologresConfigs.writeStrictDataTypeCheck &&
          getSparkDataTypesSupportImplicitCast(getSparkDataType(holoColumn), column.dataType).contains(column.dataType)) {
          logger.warn(String.format("column %s in hologres table %s type does not match: spark type: %s," +
            " hologres type: %s, but will be casted because write.strict_datatype_check is false",
            column.name, holoSchema.getTableName, column.dataType, holoColumn.getTypeName))
        } else {
          val holoTypeName = holoColumn.getTypeName + (if (holoColumn.getPrecision > 0) {
            s"(${holoColumn.getPrecision}" + (if (holoColumn.getScale > 0) {
              s",${holoColumn.getScale})"
            } else {
              ")"
            })
          } else {
            ""
          })
          throw new IllegalArgumentException(String.format("column %s in hologres table %s type does not match: spark type: %s, hologres type: %s",
            column.name, holoSchema.getTableName, column.dataType, holoTypeName))
        }
      }
    })
  }


  // 检查plan中的schema是否与sparkSchema匹配, plan中的schema只需要检查字段长度和类型
  // catalog场景写入时, 用户源表的字段名并不一定与结果表一致, 我们按照列顺序写入; 非catalog场景则根据列名写入
  def checkSparkTableSchema(hologresConfigs: HologresConfigs, sparkSchema: StructType, planSchema: StructType): Unit = {
    logger.info("spark schema: " + sparkSchema.toDDL)
    logger.info("plan schema: " + planSchema.toDDL)
    if (planSchema.length != sparkSchema.length) {
      throw new IllegalArgumentException(s"schema length not match, \nspark schema: ${sparkSchema}, " +
        s"length ${sparkSchema.length}, \nwrite schema: ${planSchema}, length ${planSchema.length}")
    }
    for (i <- 0 until planSchema.length) {
      val planType = planSchema.fields.apply(i).dataType
      val sparkType = sparkSchema.fields.apply(i).dataType
      if (!isSameDataType(planType, sparkType)) {
        if (!hologresConfigs.writeStrictDataTypeCheck && getSparkDataTypesSupportImplicitCast(sparkType, planType).contains(planType)) {
          logger.warn(String.format("column %s in hologres catalog table type does not match: spark type: %s," +
            " plan type: %s, but will be casted because write.strict_datatype_check is false",
            sparkSchema.fields.apply(i).name, sparkType, planType))
        } else {
          throw new IllegalArgumentException(s"schema not match in field ${i}, \nspark schema type: ${sparkType}," +
            s" \nwrite schema type: ${planType}, \nspark schema: ${sparkSchema}, \nwrite schema: ${planSchema}")
        }
      }
    }
  }

  /**
   * 解析 Catalog 写入实际使用的 schema。
   * 默认保持按位置全列写入；开启 write.catalog.by_name 后，按 plan 字段名映射 Hologres 列并允许部分列。
   */
  def resolveCatalogWriteSchema(
      hologresConfigs: HologresConfigs,
      sparkSchema: StructType,
      planSchema: StructType,
      holoSchema: TableSchema): StructType = {
    if (!hologresConfigs.writeCatalogByName) {
      checkSparkTableSchema(hologresConfigs, sparkSchema, planSchema)
      return sparkSchema
    }
    if (planSchema.isEmpty) {
      throw new IllegalArgumentException("write schema must contain at least one column when write.catalog.by_name is true")
    }

    val holoColumns = holoSchema.getColumnSchema
    val columnsByLowerName = holoColumns.groupBy(_.getName.toLowerCase(Locale.ROOT))
    val resolvedNames = scala.collection.mutable.HashSet.empty[String]
    val resolvedFields = planSchema.fields.map { planField =>
      val exactMatches = holoColumns.filter(_.getName == planField.name)
      val matches = if (exactMatches.nonEmpty) {
        exactMatches
      } else {
        columnsByLowerName.getOrElse(planField.name.toLowerCase(Locale.ROOT), Array.empty[Column])
      }
      if (matches.isEmpty) {
        throw new IllegalArgumentException(
          s"column ${planField.name} does not exist in hologres table ${holoSchema.getTableName}")
      }
      if (matches.length > 1) {
        throw new IllegalArgumentException(
          s"column ${planField.name} is ambiguous in hologres table ${holoSchema.getTableName}")
      }

      val holoColumn = matches.head
      if (java.lang.Boolean.TRUE.equals(holoColumn.isGeneratedColumn)) {
        throw new IllegalArgumentException(
          s"generated column ${holoColumn.getName} can not be written in hologres table ${holoSchema.getTableName}")
      }
      if (!resolvedNames.add(holoColumn.getName)) {
        throw new IllegalArgumentException(
          s"column ${holoColumn.getName} is specified more than once in catalog write schema")
      }

      val targetType = getSparkDataType(holoColumn)
      val sourceType = planField.dataType
      if (!isSameDataType(sourceType, targetType)) {
        if (!hologresConfigs.writeStrictDataTypeCheck &&
          getSparkDataTypesSupportImplicitCast(targetType, sourceType).contains(sourceType)) {
          logger.warn(String.format("column %s in hologres catalog table type does not match: spark type: %s," +
            " plan type: %s, but will be casted because write.strict_datatype_check is false",
            holoColumn.getName, targetType, sourceType))
        } else {
          throw new IllegalArgumentException(
            s"schema not match for column ${holoColumn.getName}, \nspark schema type: ${targetType}," +
              s" \nwrite schema type: ${sourceType}, \nwrite schema: ${planSchema}")
        }
      }
      StructField(holoColumn.getName, sourceType, planField.nullable, planField.metadata)
    }

    val requiredColumns = ArrayBuffer.empty[String]
    requiredColumns ++= holoSchema.getPrimaryKeys
    if (holoSchema.isPartitionParentTable) {
      requiredColumns += holoSchema.getColumn(holoSchema.getPartitionIndex).getName
    }
    val missingRequiredColumns = requiredColumns.distinct.filterNot(resolvedNames.contains)
    if (missingRequiredColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"catalog write schema is missing required column(s): ${missingRequiredColumns.mkString(", ")}")
    }

    StructType(resolvedFields.toSeq)
  }

  private[spark] def isSameDataType(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) =>
        leftElementType == rightElementType
      case _ =>
        left == right
    }
  }

  // 如果未传入spark的DDL，则根据holo表或者查询的query推断一个默认的spark schema
  def inferSparkTableSchema(holoSchema: TableSchema = null): StructType = {
    val fields = ArrayBuffer[StructField]()
    holoSchema.getColumnSchema.foreach(column => {
      fields += StructField(column.getName, getSparkDataType(column))
    })

    // Scala 2.13 下 ArrayBuffer 不再隐式转换为 Seq, 需显式 toSeq; 2.12 同样接受
    StructType(fields.toSeq)
  }

  /**
   * 三种情况:
   *  1. table参数传入holo的内表,直接返回相应的holo-client TableSchema
   *     2. table参数传入holo的view, mock一个对应的holo schema
   *     3. query参数传入的sql, mock一个对应的holo schema
   */
  def getHoloSchema(hologresConfigs: HologresConfigs): (TableSchema, String) = {
    var holoSchema: TableSchema = null
    var sourceType = "TABLE"
    var tableName: TableName = null
    var query = ""
    if (hologresConfigs.isTableConfigured) {
      @transient val holoClient = new HoloClient(hologresConfigs.holoConfig)
      try {
        holoSchema = holoClient.getTableSchema(hologresConfigs.table)
      } catch {
        case e: Exception =>
          logger.warn("get holo schema failed because:", e)
      } finally {
        holoClient.close()
      }
      if (holoSchema != null) {
        return (holoSchema, sourceType)
      } else {
        query = "select * from " + hologresConfigs.table
        sourceType = "VIEW"
        tableName = TableName.valueOf(hologresConfigs.table)
      }
    } else {
      query = hologresConfigs.query
      sourceType = "QUERY"
      tableName = TableName.quoteValueOf("", s"(${hologresConfigs.query})")
    }
    @transient val conn = JDBCUtil.createConnection(hologresConfigs)
    try {
      var holoSchema: TableSchema = null
      val metadata = conn.prepareStatement(query).getMetaData
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
      mockSchemaBuilder.setTableName(tableName)
      holoSchema = mockSchemaBuilder.build()
      holoSchema.calculateProperties()
      (holoSchema, sourceType)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(String.format("could not get metadata for query [%s]", query), e)
    } finally {
      conn.close()
    }
  }

  // 允许低精度类型写入holo高精度，一对多
  private def getSparkDataTypesSupportImplicitCast(sinkType: DataType, sourceType: DataType): Set[DataType] = {
    sinkType match {
      case DataTypes.ShortType => Set(DataTypes.ShortType)
      case DataTypes.IntegerType => Set(DataTypes.IntegerType, DataTypes.ShortType)
      case DataTypes.LongType => Set(DataTypes.LongType, DataTypes.IntegerType, DataTypes.ShortType)
      case DataTypes.FloatType => Set(DataTypes.FloatType)
      case DataTypes.DoubleType => Set(DataTypes.DoubleType, DataTypes.FloatType)
      case DataTypes.BooleanType => Set(DataTypes.BooleanType)
      case DataTypes.TimestampType => Set(DataTypes.TimestampType)
      case DataTypes.BinaryType => Set(DataTypes.BinaryType)
      case DataTypes.DateType => Set(DataTypes.DateType)
      case DataTypes.StringType =>
        Set(DataTypes.StringType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType,
          DataTypes.FloatType, DataTypes.DoubleType, DataTypes.BooleanType, DataTypes.TimestampType,
          DataTypes.DateType, sourceType match {
            case decimalType: DecimalType => DecimalType(decimalType.precision, decimalType.scale)
            case _ => DataTypes.StringType
          })
      case decimalType: DecimalType => Set(DecimalType(decimalType.precision, decimalType.scale))
      case ArrayType(DataTypes.IntegerType, true) => Set(ArrayType(DataTypes.IntegerType))
      case ArrayType(DataTypes.LongType, true) => Set(ArrayType(DataTypes.LongType))
      case ArrayType(DataTypes.FloatType, true) => Set(ArrayType(DataTypes.FloatType))
      case ArrayType(DataTypes.DoubleType, true) => Set(ArrayType(DataTypes.DoubleType))
      case ArrayType(DataTypes.BooleanType, true) => Set(ArrayType(DataTypes.BooleanType))
      case ArrayType(DataTypes.StringType, true) => Set(ArrayType(DataTypes.StringType))
      case _ => throw new IllegalArgumentException(String.format("Spark data type %s does not supported now",
        sourceType))
    }
  }

  // holo类型对应的spark类型，严格检查类型，一对一
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

  def chooseBestMode(sparkSchema: StructType, holoSchema: TableSchema, hologresConfigs: HologresConfigs): HologresConfigs = {
    if (hologresConfigs.sourceType.equals("QUERY")) {
      hologresConfigs.readMode = "select"
      logger.info(s"query source only support select mode")
      return hologresConfigs
    }
    val holoClient: HoloClient = new HoloClient(hologresConfigs.holoConfig)
    try {
      var holoVersion: HoloVersion = null
      try holoVersion = holoClient.sql[HoloVersion](getHoloVersion).get(60, TimeUnit.SECONDS)
      catch {
        case e: Exception =>
          throw new IOException("Failed to get holo version", e)
      }
      hologresConfigs.holoVersion = String.format("%s.%s.%s",
        holoVersion.getMajorVersion.toString, holoVersion.getMinorVersion.toString, holoVersion.getFixVersion.toString)
      logger.info(s"holo version: ${holoVersion}")

      // 尝试直连，无法直连则各个tasks内不需要进行尝试
      if (hologresConfigs.directConnect) {
        hologresConfigs.directConnect = JDBCUtil.couldDirectConnect(hologresConfigs)
      }

      // choose best read mode
      val supportCopyOutArrow = isCopyOutArrowSupported(holoVersion)
      if (!supportCopyOutArrow && Set("auto", "bulk_read", "bulk_read_compressed").contains(hologresConfigs.readMode)) {
        logger.warn(s"bulk read requires Hologres >= 4.1.0 or >= 4.0.18 on the 4.0 branch, " +
          s"but current version is $holoVersion, fallback to select")
        hologresConfigs.readMode = "select"
      } else if ("auto" == hologresConfigs.readMode) {
        val supportCompressed = holoVersion.compareTo(new HoloVersion(3, 0, 24)) >= 0
        hologresConfigs.readMode = if (supportCompressed) "bulk_read_compressed" else "bulk_read"
        logger.info(s"choose best read mode: ${hologresConfigs.readMode}")
      }

      if (hologresConfigs.sourceType.equals("VIEW")) {
        return hologresConfigs
      }

      val supportStage = holoVersion.compareTo(new HoloVersion(4, 1, 0)) >= 0
      // 2.2.25之后支持全字段时的bulk_load_onc_conflict, 3.1.0之后支持部分字段的bulk_load_on_conflict
      val supportBulkLoadOnConflict = holoSchema.getPrimaryKeys.length > 0 &&
        ((holoVersion.compareTo(new HoloVersion(2, 2, 25)) > 0 && sparkSchema.fields.length == holoSchema.getColumnSchema.length)
          || holoVersion.compareTo(new HoloVersion(3, 1, 0)) > 0)
      val supportBulkLoad = holoSchema.getPrimaryKeys.length == 0 && holoVersion.compareTo(new HoloVersion(2, 1, 0)) > 0
      val supportStreamCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0
      val couldReshuffle = holoSchema.getDistributionKeys.length > 0
      // choose best write mode
      if ("auto" == hologresConfigs.writeMode) {
        // Hologres >= 4.2.16, 或 4.1.x 分支 >= 4.1.35, 默认使用 stage 模式
        val defaultStage = holoVersion.compareTo(new HoloVersion(4, 2, 16)) >= 0 ||
          (holoVersion.getMajorVersion == 4 && holoVersion.getMinorVersion == 1 &&
            holoVersion.compareTo(new HoloVersion(4, 1, 35)) >= 0)
        if (defaultStage) {
          hologresConfigs.writeMode = "stage"
        } else if (supportBulkLoadOnConflict) {
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

      // 显式指定目标分区(write.target_partition_columns/values)的能力门控.
      // 此处 columns 非空即表示用户显式配置(成对校验已在 HologresConfigs 完成),
      // stage 自动模式从表元数据填充发生在更后阶段, 不受此处门控影响.
      // - stage 模式的 INSERT ... PARTITION 为通用 SQL 能力, 不依赖 Hologres 5.0
      // - COPY 类模式的 PARTITION 子句需 Hologres V5.0+
      // - insert(jdbc) 模式逐行写入, 无 PARTITION 子句概念, 不支持指定目标分区
      if (hologresConfigs.writeTargetPartitionColumns.nonEmpty) {
        if ("stage" != hologresConfigs.writeMode && holoVersion.compareTo(new HoloVersion(5, 0, 0)) < 0) {
          throw new IOException(
            "write.target_partition_columns/values in " + hologresConfigs.writeMode + " mode requires Hologres version >= 5.0, " +
              s"current version is ${hologresConfigs.holoVersion}")
        }
        if ("insert" == hologresConfigs.writeMode) {
          throw new IOException(
            "write.target_partition_columns/values is not supported in insert(jdbc) write mode, " +
              "please use stage/stream/bulk_load/bulk_load_on_conflict mode")
        }
      }

      hologresConfigs
    } finally {
      if (holoClient != null) {
        holoClient.close()
      }
    }
  }

  private[spark] def isCopyOutArrowSupported(version: HoloVersion): Boolean = {
    version.compareTo(new HoloVersion(4, 1, 0)) >= 0 ||
      (version.getMajorVersion == 4 && version.getMinorVersion == 0 &&
        version.compareTo(new HoloVersion(4, 0, 18)) >= 0)
  }

}
