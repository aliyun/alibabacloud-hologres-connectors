/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.security.InvalidParameterException;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a GET operation (similar to HBase GET).
 */
public class Get {
	Record record;
	CompletableFuture<Record> future;
	// get操作提交到holo-client的时间
	long startTime;

	public Get(Record record) {
		this(record, true);
	}

	public Get(Record record, boolean isFullColumn) {
		this.record = record;
		this.fullColumn = isFullColumn;
	}

	/**
	 * @param schema
	 * @param keys
	 * @deprecated use Get.Builder instead
	 */
	@Deprecated
	public Get(TableSchema schema, Object[] keys) {
		if (schema.getPrimaryKeys().length == 0) {
			throw new InvalidParameterException("Get must have primary key");
		}
		if (keys == null || schema.getPrimaryKeys().length != keys.length) {
			throw new InvalidParameterException("expect primary key " + schema.getPrimaryKeys().length + " but input " + (keys == null ? 0 : keys.length));
		}
		this.record = Record.build(schema);

		for (int i = 0; i < keys.length; ++i) {
			if (keys[i] == null) {
				throw new InvalidParameterException("primary key cannot be null ,index:" + i + ",name:" + schema.getPrimaryKeys()[i]);
			}
			record.setObject(record.getKeyIndex()[i], keys[i]);
		}
	}

	boolean fullColumn = true;

	public void addSelectColumn(int columnIndex) {
		if (!record.isSet(columnIndex)) {
			record.setObject(columnIndex, null);
		}
		fullColumn = false;
	}

	public void addSelectColumns(int[] columnIndex) {
		for (int i : columnIndex) {
			if (!record.isSet(i)) {
				record.setObject(i, null);
			}
		}
		fullColumn = false;
	}

	public void addSelectColumn(String columnName) {
		Integer columnIndex = getRecord().getSchema().getColumnIndex(columnName);
		if (null == columnIndex) {
			throw new InvalidParameterException("can not found column named " + columnName);
		}
		if (!record.isSet(columnIndex)) {
			record.setObject(columnIndex, null);
		}
		fullColumn = false;
	}

	public boolean isFullColumn() {
		return fullColumn;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public Record getRecord() {
		return record;
	}

	public CompletableFuture<Record> getFuture() {
		return future;
	}

	public void setFuture(CompletableFuture<Record> future) {
		this.future = future;
	}

	public static Builder newBuilder(TableSchema schema) {
		return new Builder(schema);
	}

	/**
	 * Builder of Get request.
	 */
	public static class Builder {
		private final TableSchema schema;
		private final Record record;

		private boolean fullColumn = true;

		public Builder(TableSchema schema) {
			this.schema = schema;
			this.record = Record.build(schema);
		}

		public Builder setPrimaryKey(String columnName, Object value) {
			if (!schema.isPrimaryKey(columnName)) {
				throw new InvalidParameterException(String.format("Column %s is not primary key.", columnName));
			}
			if (value == null) {
				throw new InvalidParameterException("Primary key should not be null.");
			}
			Integer index = schema.getColumnIndex(columnName);
			if (index == null) {
				throw new InvalidParameterException(String.format("Table %s does not have column %s.", schema.getTableName(), columnName));
			}
			record.setObject(index, value);
			return this;
		}

		public Builder withSelectedColumns(String[] columns) {
			for (String columnName : columns) {
				this.withSelectedColumn(columnName);
			}
			return this;
		}

		public Builder withSelectedColumn(String columnName) {
			Integer index = schema.getColumnIndex(columnName);
			if (index == null) {
				throw new InvalidParameterException(String.format("Table %s does not have column %s.", schema.getTableName(), columnName));
			}
			if (!record.isSet(index)) {
				record.setObject(index, null);
			}
			this.fullColumn = false;
			return this;
		}

		public Get build() {
			for (Integer pkIndex : schema.getKeyIndex()) {
				if (record.getObject(pkIndex) == null) {
					throw new InvalidParameterException("Primary key is not all set.");
				}
			}
			return new Get(record, fullColumn);
		}
	}
}
