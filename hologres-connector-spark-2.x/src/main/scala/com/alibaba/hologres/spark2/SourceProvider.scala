package com.alibaba.hologres.spark2

import com.alibaba.hologres.spark.BaseSourceProvider
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.utils.SparkHoloUtil
import com.alibaba.hologres.spark2.sink.HoloWriter
import com.alibaba.hologres.spark2.source.{HoloQueryReader, HoloTableReader}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConverters._

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister with WriteSupport with StreamWriteSupport with ReadSupport {
  override def shortName(): String = "hologres"

  override def createWriter(
                             writeUUID: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {
    val opts = options.asMap().asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    SparkHoloUtil.checkSparkTableSchema(hologresConfigs, schema)

    Optional.of(new HoloWriter(opts, schema, mode))
  }

  override def createStreamWriter(
                                   writeUUID: String,
                                   schema: StructType,
                                   mode: OutputMode,
                                   options: DataSourceOptions): StreamWriter = {
    val opts = options.asMap().asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    SparkHoloUtil.checkSparkTableSchema(hologresConfigs, schema)

    // stream write 只支持append
    new HoloWriter(opts, schema, SaveMode.Append)
  }

  override def createReader(sparkSchema: StructType, options: DataSourceOptions): DataSourceReader = {
    val opts = options.asMap().asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    if (hologresConfigs.isTableSource) {
      SparkHoloUtil.checkSparkTableSchema(hologresConfigs, sparkSchema)
      new HoloTableReader(hologresConfigs, sparkSchema)
    } else {
      val mockHoloSchemaForQuery = SparkHoloUtil.mockHoloSchemaForQuery(hologresConfigs)
      SparkHoloUtil.checkSparkTableSchema(hologresConfigs, sparkSchema, mockHoloSchemaForQuery)
      new HoloQueryReader(hologresConfigs, sparkSchema, mockHoloSchemaForQuery)
    }
  }

  /** 用户读取时不指定schema，则根据holo的schema生成，即读取全部字段。 */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val opts = options.asMap().asScala.toMap
    val hologresConfigs = new HologresConfigs(opts)
    if (hologresConfigs.isTableSource) {
      val sparkSchema = SparkHoloUtil.inferSparkTableSchema(hologresConfigs)
      new HoloTableReader(hologresConfigs, sparkSchema)
    } else {
      val mockHoloSchemaForQuery = SparkHoloUtil.mockHoloSchemaForQuery(hologresConfigs)
      val sparkSchema = SparkHoloUtil.inferSparkTableSchema(hologresConfigs, mockHoloSchemaForQuery)
      new HoloQueryReader(hologresConfigs, sparkSchema, mockHoloSchemaForQuery)
    }
  }
}

object SourceProvider extends BaseSourceProvider
