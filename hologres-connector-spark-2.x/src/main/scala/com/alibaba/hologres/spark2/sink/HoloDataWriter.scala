package com.alibaba.hologres.spark2.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.spark.sink.BaseHoloDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.types._

import scala.collection.immutable

/** HoloDataWriter: write and commit data. */
class HoloDataWriter(
                      table: String,
                      sourceOptions: immutable.Map[String, String],
                      sparkSchema: Option[StructType],
                      clientInstance: HoloClient)
  extends BaseHoloDataWriter(table, sourceOptions, sparkSchema, clientInstance)
    with DataWriter[InternalRow] {

  // DataWriter in spark2 not extends Closeable, need to manually call close after commit
  override def commit(): Null = {
    super.commit()
    if (clientInstance == null) {
      close()
    }
    null
  }
}
