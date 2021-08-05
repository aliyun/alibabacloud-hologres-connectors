package com.alibaba.hologres.spark2.sink

import java.util.Optional

import com.alibaba.hologres.spark.sink.BaseSourceProvider
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister with WriteSupport with StreamWriteSupport {
  override def shortName(): String = "hologres"

  override def createWriter(
                             writeUUID: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {
    val opts = options.asMap().asScala.toMap
    val table = opts.getOrElse("table",
      throw new MissingArgumentException("Missing necessary parameter 'table'."))
    val streamMode = Tuple2[Boolean, OutputMode](false, null)

    Optional.of(new HoloWriter(table, opts, Some(schema), streamMode))
  }

  override def createStreamWriter(
                                   writeUUID: String,
                                   schema: StructType,
                                   mode: OutputMode,
                                   options: DataSourceOptions): StreamWriter = {
    val opts = options.asMap().asScala.toMap
    val table = opts.getOrElse("table",
      throw new MissingArgumentException("Missing necessary parameter 'table'."))
    val streamMode = Tuple2[Boolean, OutputMode](true, mode)

    new HoloWriter(table, opts, Some(schema), streamMode)
  }
}

object SourceProvider extends BaseSourceProvider
