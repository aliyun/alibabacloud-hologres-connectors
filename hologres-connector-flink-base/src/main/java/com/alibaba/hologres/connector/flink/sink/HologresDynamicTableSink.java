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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.DirtyDataStrategy;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.StageExecutionMode;
import com.alibaba.hologres.connector.flink.config.WriteMode;
import com.alibaba.hologres.connector.flink.jdbc.HologresJDBCWriter;
import com.alibaba.hologres.connector.flink.jdbc.copy.HologresJDBCCopyWriter;
import com.alibaba.hologres.connector.flink.sink.v1.HologresSinkFunction;
import com.alibaba.hologres.connector.flink.sink.v2.HologresSink;
import com.alibaba.hologres.connector.flink.sink.v2.stage.HologresStageSink;
import com.alibaba.hologres.connector.flink.utils.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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
        if (param.getWriteMode() == WriteMode.INSERT && param.isIgnoreNullByExpr()) {
            String[] targetFieldNames =
                    SchemaUtil.getTargetFieldNames(tableSchema, targetColumnIndexs);
            boolean isFirst = true;
            StringBuilder sb = new StringBuilder();
            for (String fieldName : targetFieldNames) {
                Column column = hologresTableSchema.getColumn(fieldName);
                if (column.getPrimaryKey()) {
                    continue;
                }
                if (!isFirst) {
                    sb.append(", ");
                } else {
                    isFirst = false;
                }
                String quotedColName = IdentifierUtil.quoteIdentifier(column.getName(), true);
                sb.append(quotedColName)
                        .append("=coalesce(excluded.")
                        .append(quotedColName)
                        .append(",")
                        .append("old.")
                        .append(quotedColName)
                        .append(")");
            }
            Expression ignoreNullExpr = new Expression(sb.toString(), null);
            param.setInsertExpression(ignoreNullExpr);
        }
        // STAGE streaming mode reshuffles through Sink V2 pre-write topology.
        if (param.getWriteMode() == WriteMode.STAGE) {
            String[] targetFieldNames =
                    SchemaUtil.getTargetFieldNames(tableSchema, targetColumnIndexs);
            RowType rowType = SchemaUtil.getRowType(tableSchema, targetFieldNames);
            LogicalType[] targetFieldTypes =
                    SchemaUtil.getLogicalTypes(tableSchema, targetFieldNames);
            String qualifiedTableName = param.getTable();
            boolean isBounded = resolveStageBoundedMode(config, context);
            int shardCount = hologresTableSchema.getShardCount();
            Integer[] distributionKeyIndexes =
                    isBounded
                            ? new Integer[0]
                            : getDistributionKeyIndexes(targetFieldNames, hologresTableSchema);
            Integer sinkParallelism = config.getOptional(SINK_PARALLELISM).orElse(null);

            HologresStageSink stageSink =
                    new HologresStageSink(
                            param,
                            qualifiedTableName,
                            rowType,
                            hologresTableSchema,
                            isBounded,
                            distributionKeyIndexes,
                            targetFieldTypes);
            stageSink.setParallelism(sinkParallelism);
            return sinkParallelism == null
                    ? SinkV2Provider.of(stageSink)
                    : SinkV2Provider.of(stageSink, sinkParallelism);
        }
        if (param.isEnableReshuffleByHolo()) {
            String[] targetFieldNames =
                    SchemaUtil.getTargetFieldNames(tableSchema, targetColumnIndexs);
            RowType rowType = SchemaUtil.getRowType(tableSchema, targetFieldNames);
            LogicalType[] targetFieldTypes =
                    SchemaUtil.getLogicalTypes(tableSchema, targetFieldNames);
            int shardCount = hologresTableSchema.getShardCount();
            Integer[] distributionKeyIndexes =
                    getDistributionKeyIndexes(targetFieldNames, hologresTableSchema);
            Integer sinkParallelism = config.getOptional(SINK_PARALLELISM).orElse(null);
            HologresSink<RowData> sink =
                    new HologresSink<>(
                            param,
                            tableSchema,
                            targetColumnIndexs,
                            shardCount,
                            distributionKeyIndexes,
                            targetFieldTypes);
            sink.setParallelism(sinkParallelism);
            return sinkParallelism == null
                    ? SinkV2Provider.of(sink)
                    : SinkV2Provider.of(sink, sinkParallelism);
        }
        if (param.isUseSinkV2()) {
            HologresSink<RowData> sink = new HologresSink<>(param, tableSchema, targetColumnIndexs);
            return config.getOptional(SINK_PARALLELISM)
                    .map(parallelism -> SinkV2Provider.of(sink, parallelism))
                    .orElseGet(() -> SinkV2Provider.of(sink));
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

    private static Integer[] getDistributionKeyIndexes(
            String[] inputFieldNames, HologresTableSchema hologresTableSchema) {
        Map<String, Integer> columnNameToIndex = new HashMap<>();
        for (int i = 0; i < inputFieldNames.length; i++) {
            columnNameToIndex.put(inputFieldNames[i], i);
        }
        String[] distributionKeys = hologresTableSchema.get().getDistributionKeys();
        Integer[] indexes = new Integer[distributionKeys.length];
        for (int i = 0; i < distributionKeys.length; i++) {
            indexes[i] = columnNameToIndex.get(distributionKeys[i]);
            if (indexes[i] == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Hologres distribution key '%s' is missing from sink input fields",
                                distributionKeys[i]));
            }
        }
        return indexes;
    }

    static boolean resolveStageBoundedMode(ReadableConfig config, Context context) {
        StageExecutionMode stageExecutionMode = config.get(HologresConfigs.STAGE_EXECUTION_MODE);
        switch (stageExecutionMode) {
            case BATCH:
                return true;
            case STREAMING:
                return false;
            case AUTO:
                return context != null && context.isBounded();
            default:
                throw new ValidationException(
                        "Unsupported sink.stage.execution-mode: " + stageExecutionMode);
        }
    }
}
