package com.alibaba.hologres.client.ddl;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.CommonUtil;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.hologres.client.ddl.StatementKeywords.CALL_PROCEDURE;
import static com.alibaba.hologres.client.ddl.StatementKeywords.COMMA;
import static com.alibaba.hologres.client.ddl.StatementKeywords.CREATE;
import static com.alibaba.hologres.client.ddl.StatementKeywords.DOUBLE_QUOTES;
import static com.alibaba.hologres.client.ddl.StatementKeywords.IF_NOT_EXISTS;
import static com.alibaba.hologres.client.ddl.StatementKeywords.LEFT_BRACKET;
import static com.alibaba.hologres.client.ddl.StatementKeywords.LIST;
import static com.alibaba.hologres.client.ddl.StatementKeywords.NOT_NULL;
import static com.alibaba.hologres.client.ddl.StatementKeywords.PARTITION_BY;
import static com.alibaba.hologres.client.ddl.StatementKeywords.PRIMARY_KEY;
import static com.alibaba.hologres.client.ddl.StatementKeywords.RIGHT_BRACKET;
import static com.alibaba.hologres.client.ddl.StatementKeywords.SPACE;
import static com.alibaba.hologres.client.ddl.StatementKeywords.SQUARE_BRACKETS;
import static com.alibaba.hologres.client.ddl.StatementKeywords.TABLE;

/**
 * Util for DDL generator.
 */
public class DDLGeneratorUtil {

	public static String generateCreateTableStatement(TableSchema table) {
		StringBuilder statement = new StringBuilder();
		if (table.getNotExist()) {
			statement.append(CREATE).append(SPACE).append(TABLE).append(SPACE).append(IF_NOT_EXISTS).append(SPACE).append(String.format("%s.%s", table.getSchemaName(), table.getTableName()));
		} else {
			statement.append(CREATE).append(SPACE).append(TABLE).append(SPACE).append(String.format("%s.%s", table.getSchemaName(), table.getTableName()));
		}
		statement.append(generateTableColumnsStatement(table.getColumnSchema()));
		if (!CommonUtil.isEmpty(table.getPartitionInfo())) {
			statement.append("\n").append(generatePartitionInfo(table.getPartitionInfo(), true));
		} else {
			statement.append(";").append("\n");
		}
		statement.append(generateTableCallStatement(table));
		return statement.toString();
	}

	public static String generatesCommentStatement(TableSchema table) {

		StringBuilder statement = new StringBuilder();

		if (CommonUtil.isNotEmpty(table.getComment())) {
			statement.append(genCommentStatement(String.format("%s.%s", table.getSchemaName(), table.getTableName()), null, table.getComment()));
		}
		Column[] columns = table.getColumnSchema();
		for (Column column : columns) {
			if (CommonUtil.isNotEmpty(column.getComment())) {
				statement.append(genCommentStatement(String.format("%s.%s", table.getSchemaName(), table.getTableName()), column.getName(), column.getComment()));
			}
		}
		return statement.toString();
	}

	/**
	 * 从结构化表列数据生成表列语句.
	 *
	 * @param columns 结构化表列数据
	 * @return 表列语句
	 */
	private static String generateTableColumnsStatement(Column[] columns) {
		if (columns == null || columns.length == 0) {
			return LEFT_BRACKET + "\n" + RIGHT_BRACKET;
		}
		StringBuilder statement = new StringBuilder();
		List<String> primaryKeyList = new ArrayList<>();
		for (int i = 0; i < columns.length; i++) {
			Column column = columns[i];
			if (CommonUtil.isEmpty(column.getName())) {
				continue;
			}
			if (i == 0) {
				statement.append(SPACE).append(LEFT_BRACKET).append("\n");
			} else {
				statement.append(COMMA).append("\n");
			}
			String dataType = column.getTypeName();
			if (column.getArrayType() != null && column.getArrayType()) {
				dataType += SQUARE_BRACKETS;
			}
			statement.append(SPACE).append(column.getName()).append(SPACE).append(dataType);
			if (!column.getAllowNull()) {
				statement.append(SPACE).append(NOT_NULL);
			}
			if (column.getPrimaryKey()) {
				primaryKeyList.add(column.getName());
			}
			// 最后一列table constraint统一输出primary key
			if (i == columns.length - 1) {
				if (!primaryKeyList.isEmpty()) {
					statement.append(COMMA).append("\n").append(PRIMARY_KEY).append(SPACE).append(LEFT_BRACKET);
					statement.append(CommonUtil.join(primaryKeyList, COMMA));
					statement.append(RIGHT_BRACKET);
				}
				statement.append("\n").append(RIGHT_BRACKET);
			}
		}
		return statement.toString();
	}

	/**
	 * 生成partition语句.
	 *
	 * @param partitionInfo partition字段
	 * @return partition语句
	 */
	private static String generatePartitionInfo(String partitionInfo, boolean needList) {
		if (CommonUtil.isNotEmpty(partitionInfo)) {
			if (needList) {
				return PARTITION_BY + SPACE + LIST + LEFT_BRACKET + DOUBLE_QUOTES + partitionInfo + DOUBLE_QUOTES + RIGHT_BRACKET + ";\n";
			} else {
				return PARTITION_BY + SPACE + partitionInfo + ";\n";

			}
		}
		return "";
	}

	/**
	 * 从结构化表数据生成表call procedure的逻辑.
	 *
	 * @param table 结构化表数据
	 * @return 表with语句
	 */
	private static String generateTableCallStatement(TableSchema table) {

		String tableName = String.format("%s.%s", table.getSchemaName(), table.getTableName());

		StringBuilder statement = new StringBuilder();

		if (CommonUtil.isNotEmpty(table.getOrientation())) {
			statement.append(genCallPrecedureStatement(tableName, "orientation", table.getOrientation()));
		}
		if (CommonUtil.isNotEmpty(table.getClusteringKey())) {
			statement.append(genCallPrecedureStatement(tableName, "clustering_key", CommonUtil.join(table.getClusteringKey(), ",")));
		}
		if (CommonUtil.isNotEmpty(table.getSegmentKey())) {
			statement.append(genCallPrecedureStatement(tableName, "segment_key", CommonUtil.join(table.getSegmentKey(), ",")));
		}
		if (CommonUtil.isNotEmpty(table.getBitmapIndexKey())) {
			statement.append(genCallPrecedureStatement(tableName, "bitmap_columns", CommonUtil.join(table.getBitmapIndexKey(), ",")));
		}
		if (CommonUtil.isNotEmpty(table.getDictionaryEncoding())) {
			statement.append(genCallPrecedureStatement(tableName, "dictionary_encoding_columns", CommonUtil.join(table.getDictionaryEncoding(), ",")));
		}
		if (table.getLifecycle() != null) {
			statement.append(genCallPrecedureStatement(tableName, "time_to_live_in_seconds", table.getLifecycle().toString()));
		}
		return statement.toString();
	}

	private static String genCallPrecedureStatement(String tableName, String key, String value) {
		return String.format("%s('%s', '%s', '%s');\n", CALL_PROCEDURE, tableName, key, value);
	}

	// type只能是 column 或者 table
	public static String genCommentStatement(String tableName, String columnName, String comment) {
		if (CommonUtil.isNotEmpty(columnName)) {
			return String.format("comment on column %s.%s is '%s';\n", tableName, columnName, comment.replaceAll("'", "''"));
		} else {
			return String.format("comment on table %s is '%s';\n", tableName, comment.replaceAll("'", "''"));
		}
	}
}
