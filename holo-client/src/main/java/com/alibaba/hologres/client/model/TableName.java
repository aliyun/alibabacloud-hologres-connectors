/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.InvalidIdentifierException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * 表名.
 */
public class TableName implements Serializable {
	public static final String DEFAULT_SCHEMA_NAME = "public";
	private static final byte[] LOCK = new byte[0];
	private static Map<String, TableName> tableCache2 = new HashMap<>();
	String schemaName;
	String tableName;
	String fullName;

	private TableName(String schemaName, String tableName, String fullName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.fullName = fullName;
	}

	public String getFullName() {
		return fullName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public static TableName valueOf(String schemaName, String tableName) throws InvalidIdentifierException {
		if (schemaName == null || schemaName.length() < 1) {
			schemaName = DEFAULT_SCHEMA_NAME;
		}
		return valueOf(schemaName + "." + tableName);
	}

	static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[^\"\\s\\d\\-;][^\"\\s\\-;]*$");

	public static String parseIdentifier(String identifier) {
		if (identifier.startsWith("\"")) {
			if (identifier.endsWith("\"") && identifier.length() > 2) {
				StringBuilder sb = new StringBuilder();
				boolean isQuota = false;
				for (int i = 1; i < identifier.length() - 1; ++i) {
					char c = identifier.charAt(i);
					if ('"' == c) {
						if (isQuota) {
							sb.append(c);
						}
						isQuota = !isQuota;
					} else if (isQuota) {
						throw new InvalidIdentifierException(identifier);
					} else {
						sb.append(c);
					}
				}
				if (isQuota) {
					throw new InvalidIdentifierException(identifier);
				} else {
					return sb.toString();
				}
			}
		} else if (IDENTIFIER_PATTERN.matcher(identifier).find()) {
			return identifier.toLowerCase();
		}
		throw new InvalidIdentifierException(identifier);
	}

	public static TableName valueOf(String name) throws InvalidIdentifierException {
		TableName tableName = tableCache2.get(name);
		if (tableName == null) {
			synchronized (LOCK) {
				tableName = tableCache2.get(name);
				if (tableName == null) {
					String[] schemaAndTableName = name.split("\\.");
					String schemaName;
					String qualifierName;
					if (schemaAndTableName.length < 2) {
						schemaName = DEFAULT_SCHEMA_NAME;
						qualifierName = schemaAndTableName[0];
					} else {
						schemaName = schemaAndTableName[0];
						qualifierName = schemaAndTableName[1];
					}
					String parsedSchemaName = parseIdentifier(schemaName);
					String parsedTableName = parseIdentifier(qualifierName);
					StringBuilder sb = new StringBuilder();
					sb.append("\"");
					for (int i = 0; i < parsedSchemaName.length(); ++i) {
						char c = parsedSchemaName.charAt(i);
						if (c == '"') {
							sb.append(c);
						}
						sb.append(c);
					}
					sb.append("\".\"");
					for (int i = 0; i < parsedTableName.length(); ++i) {
						char c = parsedTableName.charAt(i);
						if (c == '"') {
							sb.append(c);
						}
						sb.append(c);
					}
					sb.append("\"");
					String parsedFullName = sb.toString();
					Map<String, TableName> temp = new HashMap<>(tableCache2);
					tableName = tableCache2.get(parsedFullName);
					if (tableName == null) {
						tableName = new TableName(parsedSchemaName, parsedTableName, parsedFullName);
						temp.put(parsedFullName, tableName);
					}
					temp.put(name, tableName);
					tableCache2 = temp;
				}
			}
		}
		return tableName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableName tableName1 = (TableName) o;
		return Objects.equals(schemaName, tableName1.schemaName)
				&& Objects.equals(tableName, tableName1.tableName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(schemaName, tableName);
	}

	@Override
	public String toString() {
		return fullName;
	}
}

