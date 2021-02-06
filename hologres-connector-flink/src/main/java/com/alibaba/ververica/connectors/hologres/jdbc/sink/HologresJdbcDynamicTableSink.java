package com.alibaba.ververica.connectors.hologres.jdbc.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Holo Table Sink.
 */
public class HologresJdbcDynamicTableSink implements DynamicTableSink, SupportsOverwrite {
	private static final Logger LOG = LoggerFactory.getLogger(HologresJdbcDynamicTableSink.class);

	private TableSchema tableSchema;
	private HologresParams params;
	private boolean overwrite = false;

	public HologresJdbcDynamicTableSink(TableSchema schema, HologresParams params) {
		this.tableSchema = checkNotNull(schema);
		this.params = checkNotNull(params);
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
		return changelogMode;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		return SinkFunctionProvider.of(new HologresJdbcSinkFunction(tableSchema, params));
	}

	@Override
	public DynamicTableSink copy() {
		return new HologresJdbcDynamicTableSink(tableSchema, params);
	}

	@Override
	public String asSummaryString() {
		return "Hologres-jdbc";
	}

	@Override
	public void applyOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
}
