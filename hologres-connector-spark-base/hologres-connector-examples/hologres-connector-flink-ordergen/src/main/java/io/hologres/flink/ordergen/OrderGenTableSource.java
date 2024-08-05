package io.hologres.flink.ordergen;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** OrderTableSource. */
public class OrderGenTableSource implements ScanTableSource {
    private final TableSchema schema;

    public OrderGenTableSource(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public DynamicTableSource copy() {
        return new OrderGenTableSource(schema);
    }

    @Override
    public String asSummaryString() {
        return "OrderGen Table Source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final SourceFunction<RowData> sourceFunction = new OrderGenSourceFunction();
        return SourceFunctionProvider.of(sourceFunction, false);
    }
}
