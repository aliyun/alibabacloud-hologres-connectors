package com.alibaba.hologres.spark3.sink

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.BaseHoloDataWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types._

/** HoloDataWriter. */
class HoloDataWriter(
                      hologresConfigs: HologresConfigs,
                      sparkSchema: StructType,
                      holoSchema: TableSchema)
  extends BaseHoloDataWriter(hologresConfigs, sparkSchema, holoSchema)
    with DataWriter[InternalRow]
