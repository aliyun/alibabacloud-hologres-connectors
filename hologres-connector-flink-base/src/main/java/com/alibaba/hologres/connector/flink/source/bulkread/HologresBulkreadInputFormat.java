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

package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Bulkread input format. */
public class HologresBulkreadInputFormat extends RichInputFormat<RowData, HologresShardInputSplit>
        implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresBulkreadInputFormat.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions options;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private transient String[] holoColumnTypes;

    private HologresBulkReader hologresBulkReader;
    private Tuple3<RowData, Integer, Long> record;
    private final String filterPredicate;
    private final long limit;
    private final int scanSplitCount;
    private long currentSplitStartMs;
    private long currentSplitRecordCount;

    public HologresBulkreadInputFormat(
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            TableSchema tableSchema,
            String filterPredicate,
            long limit,
            int scanSplitCount) {
        this.connectionParam = connectionParam;
        this.options = jdbcOptions;
        this.fieldNames = tableSchema.getFieldNames();
        this.fieldTypes = tableSchema.getFieldDataTypes();
        this.filterPredicate = filterPredicate;
        this.limit = limit;
        this.scanSplitCount = scanSplitCount;
    }

    @Override
    public HologresShardInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        // ignore minNumSplits

        LOG.info("Creating input splits for Holo shards");

        int shardCount = JDBCUtils.getShardCount(options);
        HologresShardInputSplit[] splits = createShardInputSplits(shardCount, scanSplitCount);

        LOG.info(
                "Created {} input splits for {} Holo shards, configured split count {}",
                splits.length,
                shardCount,
                scanSplitCount);

        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(
            HologresShardInputSplit[] hologresShardInputSplits) {
        return new DefaultInputSplitAssigner(hologresShardInputSplits);
    }

    @Override
    public void open(HologresShardInputSplit inputSplit) throws IOException {
        LOG.info(
                "Opening HoloShardInputSplit {}, shard ids {}",
                inputSplit.getSplitNumber(),
                Arrays.toString(inputSplit.getShardIds()));
        currentSplitStartMs = System.currentTimeMillis();
        currentSplitRecordCount = 0;
        initializeHoloColumnTypes();
        hologresBulkReader =
                new HologresBulkReader(
                        connectionParam,
                        options,
                        fieldNames,
                        fieldTypes,
                        holoColumnTypes,
                        inputSplit.getShardIds(),
                        false,
                        filterPredicate,
                        limit);
        hologresBulkReader.open();
    }

    static HologresShardInputSplit[] createShardInputSplits(
            int shardCount, int configuredSplitCount) {
        int splitCount =
                configuredSplitCount > 0 ? Math.min(configuredSplitCount, shardCount) : shardCount;
        List<HologresShardInputSplit> splits = new ArrayList<>(splitCount);
        for (int splitNumber = 0; splitNumber < splitCount; splitNumber++) {
            int startInclusive = splitNumber * shardCount / splitCount;
            int endExclusive = (splitNumber + 1) * shardCount / splitCount;
            String[] shardIds = new String[endExclusive - startInclusive];
            for (int shardId = startInclusive; shardId < endExclusive; shardId++) {
                shardIds[shardId - startInclusive] = String.valueOf(shardId);
            }
            splits.add(new HologresShardInputSplit(splitNumber, shardIds));
        }
        return splits.toArray(new HologresShardInputSplit[0]);
    }

    private void initializeHoloColumnTypes() {
        if (holoColumnTypes != null) {
            return;
        }
        HologresTableSchema hologresTableSchema =
                HologresTableSchema.get(connectionParam.getJdbcOptions());
        holoColumnTypes = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            Integer hologresColumnIndex = hologresTableSchema.get().getColumnIndex(fieldNames[i]);
            if (hologresColumnIndex == null || hologresColumnIndex < 0) {
                throw new IllegalArgumentException(
                        "Hologres table "
                                + hologresTableSchema.get().getTableName()
                                + " does not have column "
                                + fieldNames[i]);
            }
            holoColumnTypes[i] =
                    hologresTableSchema.get().getColumn(hologresColumnIndex).getTypeName();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return (record = hologresBulkReader.nextRecord()) == null;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        currentSplitRecordCount++;
        return record.f0;
    }

    @Override
    public void close() throws IOException {
        if (hologresBulkReader != null) {
            hologresBulkReader.close();
        }
        LOG.info(
                "Closed HoloShardInputSplit, loaded {} records, cost {} ms",
                currentSplitRecordCount,
                System.currentTimeMillis() - currentSplitStartMs);
    }

    @Override
    public void configure(Configuration configuration) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }
}
