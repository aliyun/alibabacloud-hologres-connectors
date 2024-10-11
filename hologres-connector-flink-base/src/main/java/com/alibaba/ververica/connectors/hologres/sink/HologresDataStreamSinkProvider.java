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

package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.data.RowData;

import java.util.function.Function;

/**
 * HologresDataStreamSinkProvider, Provider that consumes a Java DataStream as a runtime
 * implementation for DynamicTableSink, More flexible than SinkFunctionProvider.
 */
public class HologresDataStreamSinkProvider implements DataStreamSinkProvider {

    private final Function<DataStream<RowData>, DataStreamSink<?>> producer;

    public HologresDataStreamSinkProvider(
            Function<DataStream<RowData>, DataStreamSink<?>> producer) {
        this.producer = producer;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(
            ProviderContext providerContext, DataStream<RowData> dataStream) {
        return producer.apply(dataStream);
    }
}
