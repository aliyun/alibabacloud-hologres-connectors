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
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
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
        this.hologresWriter = getHologresWriter();
        this.hologresWriter.open(
                initContext.getSubtaskId(), initContext.getNumberOfParallelSubtasks());
    }

    @Override
    public void write(InputT input, Context context) throws IOException {
        long byteSize = 0;
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
        numRecordsOutCounter.inc();
        numBytesSendCounter.inc(byteSize);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        getHologresWriter().flush();
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
                            param,
                            fieldNames,
                            fieldTypes,
                            hologresTableSchema,
                            numFrontends,
                            frontendOffset);
        }
        return hologresWriter;
    }
}
