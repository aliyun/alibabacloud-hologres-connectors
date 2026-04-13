package com.alibaba.hologres.spark3.sink.copy

import com.alibaba.hologres.client.model.TableSchema
import com.alibaba.hologres.spark.config.HologresConfigs
import com.alibaba.hologres.spark.sink.copy.BaseHoloDataCopyStageWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.types._

/** HoloDataStageWriter. */
class HoloDataStageWriter(
                          hologresConfigs: HologresConfigs,
                          sparkSchema: StructType,
                          holoSchema: TableSchema,
                          stageName: String,
                          taskId: String = "")
  extends BaseHoloDataCopyStageWriter(hologresConfigs, sparkSchema, holoSchema, stageName, taskId)
    with DataWriter[InternalRow]
