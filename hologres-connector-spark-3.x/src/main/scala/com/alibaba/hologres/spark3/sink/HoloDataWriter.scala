package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.client.HoloClient
import com.alibaba.hologres.spark.sink.BaseHoloDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types._

import scala.collection.immutable

/** HoloDataWriter. */
class HoloDataWriter(
                      table: String,
                      sourceOptions: immutable.Map[String, String],
                      sparkSchema: Option[StructType],
                      clientInstance: HoloClient)
  extends BaseHoloDataWriter(table, sourceOptions, sparkSchema, clientInstance)
    with DataWriter[InternalRow] {

  override def close(): Unit = {
    if (clientInstance == null) {
      super.close()
    }
  }
}
