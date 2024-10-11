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
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.io.IOException;
import java.util.Map;

/** A {@link Sink V2} for Hologres. */
public class HologresSink<InputT> implements Sink<InputT> {

    private final HologresConnectionParam hologresConnectionParam;
    // TableSchema is not Serialiaze, just get what we need from TableSchema;
    protected final String[] fieldNames;
    protected final LogicalType[] fieldTypes;
    protected final Map<String, LogicalType> primarykeys;

    public HologresSink(
            HologresConnectionParam hologresConnectionParam,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            Map<String, LogicalType> pks) {
        this.hologresConnectionParam = hologresConnectionParam;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.primarykeys = pks;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        return new HologresSinkWriter<>(
                hologresConnectionParam, fieldNames, fieldTypes, primarykeys, initContext);
    }
}
