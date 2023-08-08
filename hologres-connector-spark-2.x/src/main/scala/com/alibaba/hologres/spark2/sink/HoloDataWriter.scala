package com.alibaba.hologres.spark2.sink

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.BaseHoloDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.types._

/** HoloDataWriter: write and commit data. */
class HoloDataWriter(
                      hologresConfigs: HologresConfigs,
                      sparkSchema: StructType,
                      holoSchema: TableSchema)
  extends BaseHoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    with DataWriter[InternalRow] {

  // DataWriter in spark2 not extends Closeable, need to manually call close after commit
  override def commit(): Null = {
    super.commit()
    close()
    null
  }
}
