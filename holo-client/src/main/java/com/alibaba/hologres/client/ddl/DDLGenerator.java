package com.alibaba.hologres.client.ddl;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.CommonUtil;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.hologres.client.ddl.StatementKeywords.BEGIN;
import static com.alibaba.hologres.client.ddl.StatementKeywords.COMMIT;
import static com.alibaba.hologres.client.ddl.StatementKeywords.SEMICOLON;


/**
 * Created by liangmei.gl.
 **/
public class DDLGenerator {

	public static String sqlGenerator(TableSchema table) throws HoloClientException {
		String errorMsg = validate(table);
		if (CommonUtil.isNotEmpty(errorMsg)) {
			throw new HoloClientException(ExceptionCode.GENERATOR_PARAMS_ERROR, errorMsg);
		}
		table.convertTableIdentifier();
		StringBuilder ddl = new StringBuilder();
		ddl.append(BEGIN + SEMICOLON + "\n");
		ddl.append(DDLGeneratorUtil.generateCreateTableStatement(table));
		ddl.append(DDLGeneratorUtil.generatesCommentStatement(table));
		ddl.append(COMMIT + SEMICOLON + "\n");
		return ddl.toString();
	}

	private static String validate(TableSchema table) {

		if (CommonUtil.isNotEmpty(table.getOrientation())) {
			if (!Arrays.asList("column", "row").contains(table.getOrientation())) {
				return "orientation error,should be column or row";
			}
		}

		String errorMsg = checkIndexInColumns(table.getBitmapIndexKey(), table.getColumnSchema(), "bitmapIndexKey", true, table.getSensitive());
		if (CommonUtil.isNotEmpty(errorMsg)) {
			return errorMsg;
		}

		errorMsg = checkIndexInColumns(table.getClusteringKey(), table.getColumnSchema(), "clusteringKey", false, table.getSensitive());
		if (CommonUtil.isNotEmpty(errorMsg)) {
			return errorMsg;
		}

		errorMsg = checkIndexInColumns(table.getDictionaryEncoding(), table.getColumnSchema(), "dictionaryEncoding", true, table.getSensitive());
		if (CommonUtil.isNotEmpty(errorMsg)) {
			return errorMsg;
		}

		errorMsg = checkIndexInColumns(table.getSegmentKey(), table.getColumnSchema(), "segmentKey", false, table.getSensitive());
		if (CommonUtil.isNotEmpty(errorMsg)) {
			return errorMsg;
		}

		errorMsg = checkIndexInColumns(table.getDistributionKeys(), table.getColumnSchema(), "distributionKey", true, table.getSensitive());
		if (CommonUtil.isNotEmpty(errorMsg)) {
			return errorMsg;
		}

		if (CommonUtil.isNotEmpty(table.getPartitionInfo())) {
			errorMsg = checkIndexInColumns(new String[]{table.getPartitionInfo()}, table.getColumnSchema(), "partitionKey", false, table.getSensitive());
			if (CommonUtil.isNotEmpty(errorMsg)) {
				return errorMsg;
			}
		}

		return null;
	}

	private static String checkIndexInColumns(String[] indexKeys, Column[] columns, String keyName, boolean allowedNull, boolean isSensitive) {

		if (indexKeys == null || indexKeys.length == 0) {
			return null;
		}

		if (columns == null || columns.length == 0) {
			return "column can't be null";
		}

		for (String key : indexKeys) {

			if (!isSensitive) {
				Map<String, Column> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
				for (Column column : columns) {
					columnMap.put(column.getName(), column);
				}

				if (!columnMap.keySet().stream().anyMatch(key::equalsIgnoreCase)) {
					return String.format("%s: %s not exists in columns", keyName, key);
				}
				if (!allowedNull && !columnMap.get(key).getAllowNull().equals(allowedNull)) {
					return String.format("%s : %s shouldn't be nullable", keyName, columnMap.get(key).getName());
				}
			} else {

				Map<String, Column> columnMap = Arrays.asList(columns).stream().collect(Collectors.toMap(Column::getName, Function.identity()));
				if (!columnMap.keySet().contains(key)) {
					return String.format("%s: %s not exists in columns", keyName, key);
				}
				if (!allowedNull && !columnMap.get(key).getAllowNull().equals(allowedNull)) {
					return String.format("%s : %s shouldn't be nullable", keyName, columnMap.get(key).getName());
				}
			}
		}

		return null;
	}
}
