package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import com.alibaba.ververica.connectors.hologres.utils.SchemaUtil;

import java.util.Random;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Holo Table Sink. */
public class HologresDynamicTableSink implements DynamicTableSink {
    private final TableSchema tableSchema;
    private final ReadableConfig config;
    private final CopyMode copyMode;
    private String[] fieldNames;
    private LogicalType[] fieldTypes;

    public HologresDynamicTableSink(TableSchema schema, ReadableConfig config) {
        this.tableSchema = checkNotNull(schema);
        this.fieldNames = schema.getFieldNames();
        this.fieldTypes = SchemaUtil.getLogicalTypes(tableSchema);
        this.config = checkNotNull(config);
        this.copyMode = config.get(HologresJDBCConfigs.COPY_WRITE_MODE);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (copyMode != null) {
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        } else {
            return ChangelogMode.upsert();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        HologresConnectionParam param = new HologresConnectionParam(config);
        HologresTableSchema hologresTableSchema = HologresTableSchema.get(param.getJdbcOptions());
        HologresWriter<RowData> hologresWriter;
        if (copyMode != null && param.isEnableReshuffleByHolo()) {
            return new HologresDataStreamSinkProvider(
                    (dataStream) -> {
                        HologresRepartitionSinkBuilder builder =
                                new HologresRepartitionSinkBuilder(
                                        param, tableSchema, hologresTableSchema);
                        builder.forRowData(
                                new DataStream<>(
                                        dataStream.getExecutionEnvironment(),
                                        dataStream.getTransformation()));
                        config.getOptional(SINK_PARALLELISM).ifPresent(builder::parallelism);
                        return builder.build();
                    });
        } else if (copyMode != null) {
            param.setDirectConnect(JDBCUtils.couldDirectConnect(param.getJdbcOptions()));
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
        } else {
            hologresWriter =
                    HologresJDBCWriter.createTableWriter(
                            param, fieldNames, fieldTypes, hologresTableSchema);
        }

        return SinkFunctionProvider.of(new HologresSinkFunction(param, hologresWriter));
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
