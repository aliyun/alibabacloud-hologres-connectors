package com.alibaba.hologres.spark3.sink.copy

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.copy.BaseHoloDataCopyWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types._

/** HoloDataCopyWriter. */
class HoloDataCopyWriter(
                          hologresConfigs: HologresConfigs,
                          sparkSchema: StructType,
                          holoSchema: TableSchema,
                          targetShardList: String = "",
                          taskId: String = "")
  extends BaseHoloDataCopyWriter(hologresConfigs, sparkSchema, holoSchema, targetShardList, taskId)
    with DataWriter[InternalRow]
