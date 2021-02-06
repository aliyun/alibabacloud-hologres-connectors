package com.alibaba.ververica.connectors.hologres.jdbc.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.common.sink.HasRetryTimeout;
import com.alibaba.ververica.connectors.common.sink.Syncable;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import com.alibaba.ververica.connectors.hologres.jdbc.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.jdbc.serde.HologresRowConverter;
import org.postgresql.core.SqlCommandType;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HologresJdbcOutputFormat.
 */
public class HologresJdbcOutputFormat extends RichOutputFormat<RowData> implements Syncable, HasRetryTimeout {
	private static final transient Logger LOG = LoggerFactory.getLogger(HologresJdbcOutputFormat.class);
	private final HologresParams params;
	private final String[] fieldNames;
	private final LogicalType[] fieldTypes;
	HoloClient client;
	HologresRowConverter converter;
	TableSchema tableSchema;

	public HologresJdbcOutputFormat(org.apache.flink.table.api.TableSchema schema, HologresParams params) {
		this.params = params;
		this.fieldNames = schema.getFieldNames();
		this.fieldTypes = getLogicalTypes(schema);
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			Class.forName("org.postgresql.Driver");
			HoloConfig holoConfig = new HoloConfig();
			JDBCOptions jdbcOptions = params.getJdbcOptions();
			holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
			holoConfig.setUsername(jdbcOptions.getUsername());
			holoConfig.setPassword(jdbcOptions.getPassword());
			if (params.isSplitStrategyKey()) {
				holoConfig.setWriteBatchByteSize(params.getWriteBatchByteSize());
			} else {
				holoConfig.setWriteBatchSize(params.getWriteBatchSize());
			}

			holoConfig.setDynamicPartition(params.isCreateMissingPartTable());
			holoConfig.setWriteMode(params.getWriteMode());
			client = new HoloClient(holoConfig);
			this.converter = new HologresRowConverter(fieldNames, fieldTypes, params);
			tableSchema = client.getTableSchema(params.getTable());
			converter.setHologresColumnSchema(tableSchema);
		} catch (ClassNotFoundException | HoloClientException e) {
			throw new IOException(e);
		}
	}

	@Override
	public long getRetryTimeout() {
		return 0;
	}

	@Override
	public void sync() throws IOException {
		try {
			client.flush();
		} catch (HoloClientException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void configure(Configuration configuration) {
	}

	@Override
	public void writeRecord(RowData value) throws IOException {
		if (null == value) {
			return;
		}
		try {
			final RowKind kind = value.getRowKind();
			Put put = new Put(tableSchema);
			Record record = put.getRecord();
			if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
				record.setType(SqlCommandType.INSERT);
			} else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE)) && !params.isIgnoreDelete()) {
				record.setType(SqlCommandType.DELETE);
			} else {
				LOG.info("Ignore rowdata {}.", value);
			}
			converter.convertToHologresRecord(record, value);
			client.put(put);
		} catch (HoloClientException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			sync();
		} finally {
			if (null != client) {
				client.close();
				client = null;
			}
		}
	}

	public LogicalType[] getLogicalTypes(org.apache.flink.table.api.TableSchema tableSchema) {
		int columnsNum = tableSchema.getFieldCount();
		String[] fieldNames = new String[columnsNum];
		LogicalType[] fieldTypes = new LogicalType[columnsNum];
		for (int idx = 0; idx < columnsNum; idx++) {
			fieldNames[idx] = tableSchema.getFieldNames()[idx];
			fieldTypes[idx] = tableSchema.getFieldDataType(fieldNames[idx]).get().getLogicalType();
		}
		return fieldTypes;
	}
}
