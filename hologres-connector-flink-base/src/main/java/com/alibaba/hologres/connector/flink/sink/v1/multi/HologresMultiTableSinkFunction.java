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

package com.alibaba.hologres.connector.flink.sink.v1.multi;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.jdbc.copy.HologresJDBCCopyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Sink Function. */
public class HologresMultiTableSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction, AutoCloseable {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresMultiTableSinkFunction.class);
    private static final String TABLE_NAME_KEY = HologresConfigs.TABLE.key();

    private final Configuration commonConfig;
    private final Map<TableName, Configuration> tableToConfig;
    private final Map<Tuple2<TableName, Set<String>>, HologresMapOutputFormat> tableToOutputFormat;
    private final Map<TableName, WriteMode> tableToWriteMode;

    private final HoloRecordConverter<T> convert;

    public HologresMultiTableSinkFunction(
            Configuration commonConfig, HoloRecordConverter<T> convert) {
        this(commonConfig, new HashMap<>(), convert);
    }

    public HologresMultiTableSinkFunction(
            Configuration commonConfig,
            Map<TableName, Configuration> tableToConfig,
            HoloRecordConverter<T> convert) {
        this.commonConfig = commonConfig;
        this.tableToConfig = tableToConfig;
        this.tableToWriteMode = new HashMap<>();
        this.tableToOutputFormat = new ConcurrentHashMap<>();
        this.convert = convert;
    }

    public void initOutputFormat(HologresMapOutputFormat format) throws IOException {
        format.setRuntimeContext(getRuntimeContext());
        RuntimeContext context = getRuntimeContext();
        int indexInSubtaskGroup = context.getTaskInfo().getIndexOfThisSubtask();
        int currentNumberOfSubtasks = context.getTaskInfo().getNumberOfParallelSubtasks();
        format.open(
                new OutputFormat.InitializationContext() {
                    @Override
                    public int getNumTasks() {
                        return currentNumberOfSubtasks;
                    }

                    @Override
                    public int getTaskNumber() {
                        return indexInSubtaskGroup;
                    }

                    @Override
                    public int getAttemptNumber() {
                        return context.getTaskInfo().getAttemptNumber();
                    }
                });
    }

    @Override
    public void invoke(T record) throws Exception {
        Map<String, Object> holoRecord = convert.convert(record);
        if (!holoRecord.containsKey(TABLE_NAME_KEY)) {
            throw new RuntimeException("Table name is not found in record: " + holoRecord);
        }
        TableName tableName = TableName.valueOf(holoRecord.get(TABLE_NAME_KEY).toString());
        holoRecord.remove(TABLE_NAME_KEY);

        tableToWriteMode.computeIfAbsent(
                tableName,
                name -> {
                    Configuration tableConf = commonConfig.clone();
                    if (tableToConfig.containsKey(name)) {
                        // 可以为表单独配置参数
                        tableConf.addAll(tableToConfig.get(name));
                    }
                    return tableConf.get(HologresConfigs.WRITE_MODE);
                });
        WriteMode writeMode = tableToWriteMode.get(tableName);
        Set<String> columnNames;
        if (writeMode.equals(WriteMode.INSERT)) {
            // INSERT 模式下, 列名不一致也可以复用同一个outputFormat
            columnNames = Collections.emptySet();
        } else {
            // COPY 模式下, 列名必须一致
            columnNames = holoRecord.keySet();
        }

        HologresMapOutputFormat format =
                tableToOutputFormat.computeIfAbsent(
                        new Tuple2<>(tableName, columnNames),
                        nameColumnsTuple -> {
                            try {
                                TableName name = nameColumnsTuple.f0;
                                Configuration tableConf = commonConfig.clone();
                                if (tableToConfig.containsKey(name)) {
                                    // 可以为表单独配置参数
                                    tableConf.addAll(tableToConfig.get(name));
                                }
                                // 如果没有特别设置参数,使用通用的参数
                                tableConf.setString(
                                        HologresConfigs.TABLE.key(), name.getFullName());

                                HologresConnectionParam param =
                                        new HologresConnectionParam(tableConf);
                                HologresWriter<Map<String, Object>> hologresWriter;
                                if (param.getWriteMode().equals(WriteMode.INSERT)) {
                                    hologresWriter =
                                            HologresJDBCWriter.createMapRecordWriter(
                                                    param,
                                                    HologresTableSchema.get(
                                                            param.getJdbcOptions()));
                                } else {
                                    hologresWriter =
                                            HologresJDBCCopyWriter.createMapRecordWriter(
                                                    param,
                                                    HologresTableSchema.get(
                                                            param.getJdbcOptions()));
                                }

                                HologresMapOutputFormat outputFormat =
                                        new HologresMapOutputFormat(param, hologresWriter);
                                initOutputFormat(outputFormat);
                                return outputFormat;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to create output format for table: " + tableName,
                                        e);
                            }
                        });

        format.writeData(holoRecord);
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        super.setRuntimeContext(context);
    }

    @Override
    public void close() throws IOException {
        LOG.info("HologresMultiTableSinkFunction start to closing");
        IOException lastException = null;
        for (Map.Entry<Tuple2<TableName, Set<String>>, HologresMapOutputFormat> entry :
                tableToOutputFormat.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                LOG.error("Error closing output format for table: " + entry.getKey(), e);
                lastException = e;
            }
        }
        tableToOutputFormat.clear();
        if (lastException != null) {
            throw lastException;
        }
        LOG.info("HologresMultiTableSinkFunction closed");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info(
                "HologresMulitTableSinkFunction start to taking snapshotState for checkpoint "
                        + context.getCheckpointId());
        IOException lastException = null;
        for (HologresMapOutputFormat format : tableToOutputFormat.values()) {
            try {
                format.flush();
            } catch (IOException e) {
                LOG.error("Error flushing output format", e);
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        LOG.info(
                "HologresMulitTableSinkFunction finished taking snapshotState for checkpoint "
                        + context.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}
}
