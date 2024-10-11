/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.utils.IdentifierUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CopyUtil.
 */
public class CopyUtil {
	public static final char QUOTE = '"';
	public static final char ESCAPE = '\\';
	public static final char NULL = 'N';
	public static final char DELIMITER = ',';
	public static final char NEWLINE = '\n';

	public static String buildCopyInSql(String tableName, List<String> columns, boolean binary, boolean withPk, WriteMode writeMode, CopyMode copyMode) {
		StringBuilder sb = new StringBuilder();
		sb.append("copy ").append(tableName).append("(");
		boolean first = true;
		for (String column : columns) {

			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append(
					IdentifierUtil.quoteIdentifier(
							column, true));
		}
		sb.append(")");
		sb.append(" from stdin with(");
		if (copyMode == CopyMode.STREAM) {
			sb.append("stream_mode true");
			if (binary) {
				sb.append(", format binary");
			} else {
				sb.append(", format csv, DELIMITER '").append(DELIMITER)
						.append("', ESCAPE '").append(ESCAPE)
						.append("', QUOTE '").append(QUOTE)
						.append("', NULL '").append(NULL).append("'");
			}
		} else {
			// 目前hologres普通copy（非stream_mode）只支持format csv
			sb.append("format csv, DELIMITER '").append(DELIMITER)
					.append("', ESCAPE '").append(ESCAPE)
					.append("', QUOTE '").append(QUOTE)
					.append("', NULL '").append(NULL).append("'");
		}
		// bulkLoad 自Hologres 2.2.25版本起支持on_conflict, 为了兼容历史版本, 分为两种模式
		if (withPk && (copyMode == CopyMode.STREAM || copyMode == CopyMode.BULK_LOAD_ON_CONFLICT)) {
			sb.append(", on_conflict ")
					.append(
							writeMode == WriteMode.INSERT_OR_IGNORE
									? "ignore"
									: "update");
		}
		sb.append(")");
		return sb.toString();
	}

	public static String buildCopyInSql(TableSchema schema, boolean binary, WriteMode mode) {
		return buildCopyInSql(schema, binary, mode, CopyMode.STREAM);
	}

	public static String buildCopyInSql(TableSchema schema, boolean binary, WriteMode mode, boolean streamMode) {
        return buildCopyInSql(schema, binary, mode, streamMode ? CopyMode.STREAM : CopyMode.BULK_LOAD);
    }

	public static String buildCopyInSql(TableSchema schema, boolean binary, WriteMode mode, CopyMode copyMode) {
		List<String> columns = Arrays.stream(schema.getColumnSchema()).map(Column::getName).collect(Collectors.toList());
		return buildCopyInSql(schema.getTableNameObj().getFullName(), columns, binary, schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0, mode, copyMode);
	}

	public static String buildCopyInSql(Record jdbcRecord, boolean binary, WriteMode mode) {
		return buildCopyInSql(jdbcRecord, binary, mode, CopyMode.STREAM);
	}

	public static String buildCopyInSql(Record jdbcRecord, boolean binary, WriteMode mode, boolean streamMode) {
        return buildCopyInSql(jdbcRecord, binary, mode, streamMode ? CopyMode.STREAM : CopyMode.BULK_LOAD);
    }

	public static String buildCopyInSql(Record jdbcRecord, boolean binary, WriteMode mode, CopyMode copyMode) {
		TableSchema schema = jdbcRecord.getSchema();
		List<String> columns = new ArrayList<>();
		for (int i = 0; i < schema.getColumnSchema().length; ++i) {
			if (jdbcRecord.isSet(i)) {
				columns.add(schema.getColumn(i).getName());
			}
		}
		// for partition table, the jdbcRecord.getTableName() is partition child table name.
		return buildCopyInSql(jdbcRecord.getTableName().getFullName(), columns, binary, schema.getPrimaryKeys() != null && schema.getPrimaryKeys().length > 0, mode, copyMode);
	}
}
