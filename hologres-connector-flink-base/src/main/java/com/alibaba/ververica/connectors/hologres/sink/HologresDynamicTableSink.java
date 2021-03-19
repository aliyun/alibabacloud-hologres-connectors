package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Holo Table Sink. */
public class HologresDynamicTableSink implements DynamicTableSink {
    private TableSchema tableSchema;
    private ReadableConfig config;

    public HologresDynamicTableSink(TableSchema schema, ReadableConfig config) {
        this.tableSchema = checkNotNull(schema);
        this.config = checkNotNull(config);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // UPSERT mode
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        HologresConnectionParam hologresConnectionParam = new HologresConnectionParam(config);
        HologresTableSchema hologresTableSchema =
                HologresTableSchema.get(hologresConnectionParam.getJdbcOptions());
        HologresWriter<RowData> hologresWriter;
        hologresWriter =
                HologresJDBCWriter.createTableWriter(
                        hologresConnectionParam, tableSchema, hologresTableSchema);
        return SinkFunctionProvider.of(
                new HologresSinkFunction(hologresConnectionParam, hologresWriter));
    }

    @Override
    public DynamicTableSink copy() {
        return new HologresDynamicTableSink(tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "Hologres";
    }
}
