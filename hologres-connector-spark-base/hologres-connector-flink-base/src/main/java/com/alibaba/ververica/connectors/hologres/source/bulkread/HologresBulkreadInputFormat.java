/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.source.bulkread;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.alibaba.ververica.connectors.common.MetricUtils;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/** Bulkread input format. */
public class HologresBulkreadInputFormat extends RichInputFormat<RowData, HologresShardInputSplit>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresBulkreadInputFormat.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions options;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private HologresBulkReader hologresBulkReader;
    private RowData record;
    private transient Meter sourceInTps;

    public HologresBulkreadInputFormat(
            HologresConnectionParam connectionParam, JDBCOptions options, TableSchema schema) {
        this.connectionParam = connectionParam;
        this.options = options;
        this.fieldNames = schema.getFieldNames();
        this.fieldTypes = schema.getFieldDataTypes();
    }

    @Override
    public HologresShardInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        // ignore minNumSplits

        LOG.info("Creating input splits for Holo shards");

        int numShards = JDBCUtils.getShardCount(options);

        HologresShardInputSplit[] splits = new HologresShardInputSplit[numShards];

        for (int i = 0; i < numShards; i++) {
            splits[i] = new HologresShardInputSplit(i);
        }

        LOG.info("Created {} input splits for Holo shards", splits.length);

        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(
            HologresShardInputSplit[] hologresShardInputSplits) {
        return new DefaultInputSplitAssigner(hologresShardInputSplits);
    }

    @Override
    public void open(HologresShardInputSplit inputSplit) throws IOException {
        sourceInTps = MetricUtils.registerNumRecordsInRate(getRuntimeContext());

        LOG.info("Opening HoloShardInputSplit {}", inputSplit.getSplitNumber());
        hologresBulkReader =
                new HologresBulkReader(
                        connectionParam,
                        options,
                        fieldNames,
                        fieldTypes,
                        new String[] {String.valueOf(inputSplit.getSplitNumber())});
        hologresBulkReader.open();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return (record = hologresBulkReader.nextRecord()) == null;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        sourceInTps.markEvent();
        return record;
    }

    @Override
    public void close() throws IOException {
        if (hologresBulkReader != null) {
            hologresBulkReader.close();
        }
    }

    @Override
    public void configure(Configuration configuration) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }
}
