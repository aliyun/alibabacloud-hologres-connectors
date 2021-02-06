package com.alibaba.ververica.connectors.hologres.jdbc.config;

import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.ververica.connectors.common.source.resolver.DirtyDataStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.util.JDBCUtils;

import java.io.Serializable;

import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.OPTIONAL_SPLIT_DATA_SIZE;
import static com.alibaba.ververica.connectors.hologres.jdbc.config.HologresJdbcConfigs.OPTIONAL_SPLIT_LENGTH;

/**
 * HologresParams.
 */
public class HologresParams implements Serializable {
	boolean ignoreDelete;
	DirtyDataStrategy dirtyDataStrategy = DirtyDataStrategy.EXCEPTION;
	String delimiter;
	JDBCOptions jdbcOptions;
	boolean splitStrategyKey;
	int writeBatchSize;
	long writeBatchByteSize;
	boolean createMissingPartTable;
	WriteMode writeMode;

	public HologresParams(ReadableConfig config) {
		jdbcOptions = JDBCUtils.getJDBCOptions(config);
		ignoreDelete = config.get(HologresJdbcConfigs.OPTIONAL_SINK_IGNORE_DELETE);

		String actionOnInsertError = config.get(HologresJdbcConfigs.ACTION_ON_INSERT_ERROR);
		if (actionOnInsertError.equalsIgnoreCase("SKIP")) {
			dirtyDataStrategy = DirtyDataStrategy.SKIP;
		} else if (actionOnInsertError.equalsIgnoreCase("SKIP_SILENT")) {
			dirtyDataStrategy = DirtyDataStrategy.SKIP_SILENT;
		}
		// batch param
		splitStrategyKey = config.get(HologresJdbcConfigs.OPTIONAL_SPLIT_STRATEGY);
		writeBatchByteSize = config.get(OPTIONAL_SPLIT_DATA_SIZE);
		writeBatchSize = config.get(OPTIONAL_SPLIT_LENGTH);
		createMissingPartTable = config.get(HologresJdbcConfigs.CREATE_MISSING_PARTITION_TABLE);
		delimiter = config.get(HologresJdbcConfigs.ARRAY_DELIMITER);
		writeMode = getWriteMode(config);
	}

	public String getTable() {
		return jdbcOptions.getTable();
	}

	public JDBCOptions getJdbcOptions() {
		return jdbcOptions;
	}

	public boolean isSplitStrategyKey() {
		return splitStrategyKey;
	}

	public int getWriteBatchSize() {
		return writeBatchSize;
	}

	public long getWriteBatchByteSize() {
		return writeBatchByteSize;
	}

	public boolean isCreateMissingPartTable() {
		return createMissingPartTable;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public boolean isIgnoreDelete() {
		return ignoreDelete;
	}

	public DirtyDataStrategy getDirtyDataStrategy() {
		return dirtyDataStrategy;
	}

	public WriteMode getWriteMode() {
		return this.writeMode;
	}

	private static WriteMode getWriteMode(ReadableConfig config) {
		WriteMode writeMode = WriteMode.INSERT_OR_IGNORE;
		if (config.get(HologresJdbcConfigs.INSERT_OR_UPDATE)) {
			writeMode = WriteMode.INSERT_OR_UPDATE;
		}
		if (config.getOptional(HologresJdbcConfigs.MUTATE_TYPE).isPresent()) {
			String mutateType =
					config.get(HologresJdbcConfigs.MUTATE_TYPE).toLowerCase();
			switch (mutateType) {
				case "insertorignore":
					writeMode = WriteMode.INSERT_OR_IGNORE;
					break;
				case "insertorreplace":
					writeMode = WriteMode.INSERT_OR_REPLACE;
					break;
				case "insertorupdate":
					writeMode = WriteMode.INSERT_OR_UPDATE;
					break;
				default:
					throw new RuntimeException("Could not recognize mutate type " + mutateType);
			}
		}
		return writeMode;
	}

}
