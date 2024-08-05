package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;

import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.hologres.api.HologresIOClient;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

/** Sink Function. */
public class HologresSinkFunction extends OutputFormatSinkFunction<RowData> {
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
}
