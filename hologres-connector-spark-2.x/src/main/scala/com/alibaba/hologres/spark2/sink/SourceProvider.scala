package com.alibaba.hologres.spark2.sink

import java.util.Optional

import com.alibaba.hologres.spark.sink.BaseSourceProvider
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** SourceProvider Register. */
class SourceProvider extends DataSourceRegister with WriteSupport {
  override def shortName(): String = "hologres"

  override def createWriter(
                             writeUUID: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {
    val opts = options.asMap().asScala.toMap
    val table = opts.getOrElse("table",
      throw new MissingArgumentException("Missing necessary parameter 'table'."))
    Optional.of(new HoloWriter(table, opts, Some(schema)))
  }
}

object SourceProvider extends BaseSourceProvider
