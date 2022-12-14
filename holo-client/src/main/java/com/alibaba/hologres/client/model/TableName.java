/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.InvalidIdentifierException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
		List<String> ret = parseMultiIdentifier(identifier);
		if (ret == null) {
			return null;
		} else if (ret.size() == 1) {
			return ret.get(0);
		} else {
			throw new InvalidIdentifierException(identifier);
		}
	}

	public static final char QUOTE = '"';
	public static final char SPLIT = '.';

	public static List<String> parseMultiIdentifier(String identifier) {
		if (identifier == null) {
			return null;
		}
		List<String> ret = new ArrayList<>();
		boolean isQuoteState = false;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < identifier.length(); ++i) {
			char c = identifier.charAt(i);
			if (isQuoteState) {
				if (c == QUOTE) {
					if (i < identifier.length() - 1) {
						if (identifier.charAt(i + 1) == QUOTE) {
							sb.append(QUOTE);
						} else if (identifier.charAt(i + 1) == SPLIT) {
							isQuoteState = false;
							ret.add(sb.toString());
							sb.setLength(0);
						} else {
							throw new InvalidIdentifierException(identifier);
						}
						++i;
					} else {
						ret.add(sb.toString());
						sb.setLength(0);
					}
				} else {
					sb.append(c);
				}
			} else {
				char lowerC = (char) (c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c);
				if (sb.length() == 0) {
					if (c == QUOTE) {
						isQuoteState = true;
					} else if (c == SPLIT) {
						throw new InvalidIdentifierException(identifier);
					} else {
						sb.append(lowerC);
					}
				} else {
					if (c == QUOTE) {
						throw new InvalidIdentifierException(identifier);
					} else if (c == SPLIT) {
						String text = sb.toString();
						sb.setLength(0);
						if (IDENTIFIER_PATTERN.matcher(text).find()) {
							ret.add(text);
						} else {
							throw new InvalidIdentifierException(identifier);
						}
					} else {
						sb.append(lowerC);
					}
				}
			}
		}

		if (sb.length() > 0) {
			if (isQuoteState) {
				throw new InvalidIdentifierException(identifier);
			}
			String text = sb.toString();
			sb.setLength(0);
			if (IDENTIFIER_PATTERN.matcher(text).find()) {
				ret.add(text);
			} else {
				throw new InvalidIdentifierException(identifier);
			}
		}
		return ret;
	}

	public static TableName valueOf(String name) throws InvalidIdentifierException {
		TableName tableName = tableCache2.get(name);
		if (tableName == null) {
			synchronized (LOCK) {
				tableName = tableCache2.get(name);
				if (tableName == null) {
					List<String> schemaAndTableName = parseMultiIdentifier(name);
					String parsedSchemaName;
					String parsedTableName;
					if (schemaAndTableName.size() == 1) {
						parsedSchemaName = DEFAULT_SCHEMA_NAME;
						parsedTableName = schemaAndTableName.get(0);
					} else if (schemaAndTableName.size() == 2) {
						parsedSchemaName = schemaAndTableName.get(0);
						parsedTableName = schemaAndTableName.get(1);
					} else {
						throw new InvalidIdentifierException(name);
					}
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

