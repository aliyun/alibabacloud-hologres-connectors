package com.alibaba.hologres.spark2.sink.copy

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.copy.BaseHoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.types._

/** HoloDataCopyWriter: write and commit data. */
class HoloDataCopyWriter(
                          hologresConfigs: HologresConfigs,
                          sparkSchema: Option[StructType],
                          holoSchema: TableSchema)
  extends BaseHoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema)
    with DataWriter[InternalRow] {

  // DataWriter in spark2 not extends Closeable, need to manually call close after commit
  override def commit(): Null = {
    super.commit()
    close()
    null
  }
}
