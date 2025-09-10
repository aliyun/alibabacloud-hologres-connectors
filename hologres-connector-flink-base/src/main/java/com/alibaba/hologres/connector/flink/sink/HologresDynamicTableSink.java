package com.alibaba.hologres.connector.flink.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.DirtyDataStrategy;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.hologres.connector.flink.sink.v1.HologresSinkFunction;
import com.alibaba.hologres.connector.flink.sink.v2.HologresSink;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Holo Table Sink. */
public class HologresDynamicTableSink implements DynamicTableSink {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresDynamicTableSink.class);

    private String tableName;
    private final TableSchema tableSchema;
    private HologresConnectionParam connectionParam;
    private final ReadableConfig config;
    private final WriteMode writeMode;
    private final boolean enablePartialInsert;

    public HologresDynamicTableSink(String tableName, TableSchema schema, ReadableConfig config) {
        this.tableName = tableName;
        this.tableSchema = checkNotNull(schema);
        this.config = checkNotNull(config);
        this.connectionParam = new HologresConnectionParam(config);
        this.writeMode = connectionParam.getWriteMode();
        this.enablePartialInsert = config.get(HologresConfigs.ENABLE_PARTIAL_INSERT);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (writeMode == WriteMode.INSERT) {
            return ChangelogMode.upsert();
        } else {
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Integer[] targetColumnIndexs;
        if (Objects.nonNull(context)) {
            Optional<int[][]> targetColumns = context.getTargetColumns();
            if (enablePartialInsert && targetColumns.isPresent()) {
                targetColumnIndexs = new Integer[targetColumns.get().length];
                for (int i = 0; i < targetColumns.get().length; i++) {
                    int[] column = targetColumns.get()[i];
                    if (column.length != 1) {
                        throw new ValidationException(
                                "Hologres sink table doesn’t support nested columns when enabling partial insert.");
                    }
                    targetColumnIndexs[i] = column[0];
                }
            } else {
                targetColumnIndexs = new Integer[0];
            }
        } else {
            targetColumnIndexs = new Integer[0];
        }
        HologresConnectionParam param = new HologresConnectionParam(config);
        HologresTableSchema hologresTableSchema = HologresTableSchema.get(param.getJdbcOptions());

        if (writeMode != WriteMode.INSERT
                && !param.getDirtyDataStrategy().equals(DirtyDataStrategy.EXCEPTION)) {
            throw new UnsupportedOperationException(
                    "only support EXCEPTION dirty data strategy when copy mode is enabled");
        }
        if (param.isUseSinkV2() || param.isEnableReshuffleByHolo()) {
            HologresSink<RowData> sink =
                    new HologresSink<>(param, tableSchema, hologresTableSchema, targetColumnIndexs);
            if (config.getOptional(SINK_PARALLELISM).isPresent()) {
                sink.setParallelism(config.getOptional(SINK_PARALLELISM).get());
                return SinkV2Provider.of(sink, config.getOptional(SINK_PARALLELISM).get());
            } else {
                return SinkV2Provider.of(sink);
            }
        }
        HologresWriter<RowData> hologresWriter;
        String[] targetFieldNames = SchemaUtil.getTargetFieldNames(tableSchema, targetColumnIndexs);
        LogicalType[] targetFieldTypes = SchemaUtil.getLogicalTypes(tableSchema, targetFieldNames);
        if (param.getWriteMode() == WriteMode.INSERT) {
            hologresWriter =
                    HologresJDBCWriter.createTableWriter(
                            param, targetFieldNames, targetFieldTypes, hologresTableSchema);

        } else {
            hologresWriter =
                    HologresJDBCCopyWriter.createRowDataWriter(
                            param, targetFieldNames, targetFieldTypes, hologresTableSchema);
        }

        return SinkFunctionProvider.of(new HologresSinkFunction(param, hologresWriter));
    }

    @Override
    public DynamicTableSink copy() {
        return new HologresDynamicTableSink(tableName, tableSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "Hologres-" + tableName;
    }
}
