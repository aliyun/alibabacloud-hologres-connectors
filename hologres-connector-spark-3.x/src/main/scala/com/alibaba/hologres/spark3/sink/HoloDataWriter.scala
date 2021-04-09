package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.spark.sink.BaseHoloDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types._

import scala.collection.immutable

/** HoloDataWriter. */
class HoloDataWriter(
                      table: String,
                      sourceOptions: immutable.Map[String, String],
                      sparkSchema: Option[StructType])
  extends BaseHoloDataWriter(table, sourceOptions, sparkSchema)
    with DataWriter[InternalRow]
