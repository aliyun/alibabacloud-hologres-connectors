/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.connector.flink.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.DirtyDataStrategy;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.hologres.connector.flink.sink.repartition.RowDataShardUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** A {@link SinkWriter V2} for Hologres. */
public class HologresSinkWriter<InputT> implements StatefulSinkWriter<InputT, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(HologresSinkWriter.class);
    protected final String[] targetFieldNames;
    protected final LogicalType[] targetFieldTypes;
    protected final Integer[] targetColumnIndexs;
    protected final Map<String, LogicalType> primarykeys;

    private HologresWriter hologresWriter;
    private final HologresConnectionParam param;
    private final int shardCount;
    private final Integer[] distributionKeyIndexes;
    private final LogicalType[] routingFieldTypes;
    private final int subtaskId;
    private final int numberOfParallelSubtasks;
    private boolean routingInitialized;

    private final Counter numRecordsOutCounter;
    private final Counter numBytesOutCounter;
    protected Counter sinkSkipCounter;

    protected DirtyDataStrategy dirtyDataStrategy;
    protected Tuple2<String, Exception> exception = null;

    public HologresSinkWriter(
            HologresConnectionParam hologresConnectionParam,
            String[] targetFieldNames,
            LogicalType[] targetFieldTypes,
            Integer[] targetColumnIndexs,
            Map<String, LogicalType> primarykeys,
            int shardCount,
            Integer[] distributionKeyIndexes,
            LogicalType[] routingFieldTypes,
            Sink.InitContext initContext)
            throws IOException {
        this.param = hologresConnectionParam;
        this.targetFieldNames = targetFieldNames;
        this.targetFieldTypes = targetFieldTypes;
        this.targetColumnIndexs = targetColumnIndexs;
        this.primarykeys = primarykeys;
        this.shardCount = shardCount;
        this.distributionKeyIndexes = distributionKeyIndexes;
        this.routingFieldTypes = routingFieldTypes;
        this.subtaskId = initContext.getTaskInfo().getIndexOfThisSubtask();
        this.numberOfParallelSubtasks = initContext.getTaskInfo().getNumberOfParallelSubtasks();
        this.numBytesOutCounter =
                initContext.metricGroup().getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter =
                initContext.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        this.sinkSkipCounter = initContext.metricGroup().getNumRecordsOutErrorsCounter();
        this.hologresWriter = getHologresWriter();
        this.hologresWriter.open(subtaskId, numberOfParallelSubtasks);
        this.dirtyDataStrategy = param.getDirtyDataStrategy();
    }

    @Override
    public void write(InputT input, Context context) throws IOException {
        long byteSize = 0;
        Object record = input;
        Integer targetShard = null;
        boolean addRecord =
                !(input instanceof RowData)
                        || ((RowData) input).getRowKind().equals(RowKind.INSERT)
                        || ((RowData) input).getRowKind().equals(RowKind.UPDATE_AFTER);
        if (addRecord
                && !routingInitialized
                && input instanceof RowData
                && param.isEnableReshuffleByHolo()) {
            if (distributionKeyIndexes.length > 0) {
                targetShard =
                        RowDataShardUtil.getShardId(
                                (RowData) input,
                                distributionKeyIndexes,
                                routingFieldTypes,
                                shardCount);
            } else if (shardCount > 0) {
                targetShard = subtaskId % shardCount;
                LOG.info(
                        "HologresSinkWriter: table has no distribution key, using subtask fallback"
                                + " targetShard={}, subtaskId={}, parallelism={}",
                        targetShard,
                        subtaskId,
                        numberOfParallelSubtasks);
            }
            routingInitialized = true;
        }
        try {
            if (record instanceof RowData) {
                RowKind kind = ((RowData) record).getRowKind();
                if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
                    byteSize = writeAddRecord(record, targetShard);
                } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))
                        && !param.isIgnoreDelete()) {
                    byteSize = hologresWriter.writeDeleteRecord(record);
                } else {
                    LOG.debug("Ignore rowdata {}.", record);
                }
            } else {
                byteSize = hologresWriter.writeAddRecord(record);
            }
        } catch (HoloClientException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    input,
                    ExceptionUtils.getStackTrace(e));
            // Only a specific ERROR_CODE indicates dirty data, so dirtyDataStrategy can choose to
            // ignore the exception. Other exceptions cannot be skipped.
            if (dirtyDataStrategy.equals(DirtyDataStrategy.SKIP)
                    && (e.getCode() == ExceptionCode.DATA_VALUE_ERROR
                            || e.getCode() == ExceptionCode.DATA_TYPE_ERROR)) {
                sinkSkipCounter.inc();
            } else {
                throw new IOException(e);
            }
        } catch (IOException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    input,
                    ExceptionUtils.getStackTrace(e));
            throw e;
        }
        numRecordsOutCounter.inc();
        numBytesOutCounter.inc(byteSize);
    }

    @SuppressWarnings("unchecked")
    private long writeAddRecord(Object record, Integer targetShard)
            throws IOException, HoloClientException {
        if (targetShard != null && hologresWriter instanceof HologresJDBCCopyWriter) {
            return ((HologresJDBCCopyWriter<Object>) hologresWriter)
                    .writeAddRecord(record, targetShard);
        }
        return hologresWriter.writeAddRecord(record);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        LOG.info("start to wait request to finish");
        try {
            getHologresWriter().flush();
            LOG.info("end to wait request to finish");
        } catch (HoloClientException e) {
            LOG.error("Flush messages failed", e);
            // Only a specific ERROR_CODE indicates dirty data, so dirtyDataStrategy can choose to
            // ignore the exception. Other exceptions cannot be skipped.
            if (dirtyDataStrategy.equals(DirtyDataStrategy.SKIP)
                    && (e.getCode() == ExceptionCode.DATA_VALUE_ERROR
                            || e.getCode() == ExceptionCode.DATA_TYPE_ERROR)) {
                sinkSkipCounter.inc();
            } else {
                throw new IOException(e);
            }
        } catch (IOException e) {
            LOG.error("Flush messages failed", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (hologresWriter != null) {
            hologresWriter.flush();
            hologresWriter.close();
            hologresWriter = null;
        }
    }

    private HologresWriter getHologresWriter() {
        if (hologresWriter == null) {
            HologresTableSchema hologresTableSchema =
                    HologresTableSchema.get(param.getJdbcOptions());
            if (param.getWriteMode() == WriteMode.INSERT) {
                hologresWriter =
                        HologresJDBCWriter.createTableWriter(
                                param, targetFieldNames, targetFieldTypes, hologresTableSchema);
            } else {

                hologresWriter =
                        HologresJDBCCopyWriter.createRowDataWriter(
                                param, targetFieldNames, targetFieldTypes, hologresTableSchema);
            }
        }
        return hologresWriter;
    }

    @Override
    public List<Long> snapshotState(long checkpointId) throws IOException {
        // SinkWriter基类在checkpoint时也会调用flush方法,但这里仍然使用StatefulSinkWriter以进行更好的控制
        // 目前并不真正保存状态, 仅在checkpoint时保证flush成功
        flush(false);
        return Collections.singletonList(checkpointId);
    }
}
