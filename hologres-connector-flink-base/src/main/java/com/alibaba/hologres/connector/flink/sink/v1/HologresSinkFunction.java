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

package com.alibaba.hologres.connector.flink.sink.v1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.connector.flink.api.HologresIOClient;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;

/** Sink Function. */
public class HologresSinkFunction extends OutputFormatSinkFunction<RowData>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private HologresWriter<RowData> hologresIOClient;

    public HologresSinkFunction(
            HologresConnectionParam connectionParam, HologresWriter<RowData> hologresIOClient) {
        super(new HologresTableOutputFormat(connectionParam, hologresIOClient));
        this.hologresIOClient = hologresIOClient;
    }

    @VisibleForTesting
    protected HologresIOClient getHologresIOClient() {
        return hologresIOClient;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        ((HologresTableOutputFormat) getFormat()).flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}
}
