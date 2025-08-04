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

package com.alibaba.ververica.connectors.hologres.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.common.source.resolver.DirtyDataStrategy;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

/** A {@link SinkWriter V2} for Hologres. */
public class HologresSinkWriter<InputT> implements SinkWriter<InputT> {
    private static final Logger LOG = LoggerFactory.getLogger(HologresSinkWriter.class);
    protected final String[] fieldNames;
    protected final LogicalType[] fieldTypes;
    protected final Map<String, LogicalType> primarykeys;

    private HologresWriter hologresWriter;
    private final HologresConnectionParam param;

    private final Counter numRecordsOutCounter;
    private final Counter numBytesSendCounter;
    protected Counter sinkSkipCounter;

    protected DirtyDataStrategy dirtyDataStrategy;
    protected Tuple2<String, Exception> exception = null;

    public HologresSinkWriter(
            HologresConnectionParam hologresConnectionParam,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            Map<String, LogicalType> primarykeys,
            Sink.InitContext initContext)
            throws IOException {
        this.param = hologresConnectionParam;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.primarykeys = primarykeys;
        this.numRecordsOutCounter = initContext.metricGroup().getNumRecordsSendCounter();
        this.numBytesSendCounter = initContext.metricGroup().getNumBytesSendCounter();
        this.sinkSkipCounter = initContext.metricGroup().getNumRecordsOutErrorsCounter();
        this.hologresWriter = getHologresWriter();
        this.hologresWriter.open(
                initContext.getSubtaskId(), initContext.getNumberOfParallelSubtasks());
        this.dirtyDataStrategy = param.getDirtyDataStrategy();
    }

    @Override
    public void write(InputT input, Context context) throws IOException {
        long byteSize = 0;
        try {
            if (input instanceof RowData) {
                RowKind kind = ((RowData) input).getRowKind();
                if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
                    byteSize = hologresWriter.writeAddRecord(input);
                } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))
                        && !param.isIgnoreDelete()) {
                    byteSize = hologresWriter.writeDeleteRecord(input);
                } else {
                    LOG.debug("Ignore rowdata {}.", input);
                }
            } else {
                byteSize = hologresWriter.writeAddRecord(input);
            }
        } catch (HoloClientException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    input,
                    ExceptionUtils.getStackTrace(e));
            // Only a specific ERROR_CODE indicates dirty data, so dirtyDataStrategy can choose to
            // ignore the exception. Other exceptions cannot be skipped.
            if ((dirtyDataStrategy.equals(DirtyDataStrategy.SKIP)
                            || dirtyDataStrategy.equals(DirtyDataStrategy.SKIP_SILENT))
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
        numBytesSendCounter.inc(byteSize);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        LOG.info("start to wait request to finish");
        try {
            getHologresWriter().flush();
            LOG.info("end to wait request to finish");
        } catch (HoloClientException e) {
            LOG.error("Flush messages failed", e);
            throw new IOException(e);
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
            int numFrontends = JDBCUtils.getNumberFrontends(param.getJdbcOptions());
            int frontendOffset =
                    numFrontends > 0 ? (Math.abs(new Random().nextInt()) % numFrontends) : 0;
            hologresWriter =
                    HologresJDBCCopyWriter.createRowDataWriter(
                            param, fieldNames, fieldTypes, hologresTableSchema);
        }
        return hologresWriter;
    }
}
