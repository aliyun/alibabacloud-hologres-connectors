/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import org.postgresql.model.TableSchema;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Class to represent a Select operation (similar to select a,b,c from xxx where a=? and b=?).
 */
public class Scan {

	private final TableSchema schema;
	private List<Filter> filterList;
	private BitSet selectedColumns = null;
	private final int fetchSize;
	private final int timeout;
	private final SortKeys sortKeys;
	public Scan(TableSchema schema, List<Filter> filterList, BitSet selectedColumns, int fetchSize, int timeout, SortKeys sortKeys) {
		this.schema = schema;
		this.filterList = filterList;
		this.selectedColumns = selectedColumns;
		this.fetchSize = fetchSize;
		this.timeout = timeout;
		this.sortKeys = sortKeys;
	}

	public TableSchema getSchema() {
		return schema;
	}

	public List<Filter> getFilterList() {
		return filterList;
	}

	public BitSet getSelectedColumns() {
		return selectedColumns;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public int getTimeout() {
		return timeout;
	}

	public SortKeys getSortKeys() {
		return sortKeys;
	}

	public static Builder newBuilder(TableSchema schema) {
		return new Builder(schema);
	}

	/**
	 * Builder of Get request.
	 */
	public static class Builder {

		private final TableSchema schema;
		private List<Filter> filterList;
		private BitSet selectedColumns = null;
		private int fetchSize = -1;
		private int timeout = -1;
		private SortKeys sortKeys = SortKeys.PRIMARY_KEY;

		public Builder(TableSchema schema) {
			this.schema = schema;
		}

		public Builder addEqualFilter(String columnName, Object value) {
			Integer index = schema.getColumnIndex(columnName);
			if (index == null) {
				throw new InvalidParameterException(String.format("Table %s does not have column %s.", schema.getTableName(), columnName));
			}
			if (value == null) {
				throw new InvalidParameterException(String.format("Scan can not set null value for addEqualFilter. table %s ,column %s.", schema.getTableName(), columnName));
			}
			if (filterList == null) {
				filterList = new ArrayList<>();
			}
			filterList.add(new EqualsFilter(index, value));
			return this;
		}

		/**
		 * columnName >= start and columnName < end.
		 *
		 * @param columnName columnName
		 * @param start      nullable
		 * @param end        nullable
		 * @return this
		 */
		public Builder addRangeFilter(String columnName, Object start, Object end) {
			Integer index = schema.getColumnIndex(columnName);
			if (index == null) {
				throw new InvalidParameterException(String.format("Table %s does not have column %s.", schema.getTableName(), columnName));
			}
			if (start == null && end == null) {
				throw new InvalidParameterException(String.format("Scan can not set start and end both null. table %s ,column %s.", schema.getTableName(), columnName));
			}
			if (filterList == null) {
				filterList = new ArrayList<>();
			}
			filterList.add(new RangeFilter(index, start, end));
			return this;
		}

		public Builder withSelectedColumns(String[] columns) {
			for (String columnName : columns) {
				Integer index = schema.getColumnIndex(columnName);
				if (index == null) {
					throw new InvalidParameterException(String.format("Table %s does not have column %s.", schema.getTableName(), columnName));
				}
			}
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
			if (selectedColumns == null) {
				selectedColumns = new BitSet(schema.getColumnSchema().length);
			}
			selectedColumns.set(index);
			return this;
		}

		public Builder setFetchSize(int fetchSize) {
			if (fetchSize <= 0) {
				throw new InvalidParameterException(String.format("FetchSize must > 0 . Table %s.", schema.getTableName()));
			}
			this.fetchSize = fetchSize;
			return this;
		}

		public Builder setTimeout(int timeout) {
			if (timeout <= 0) {
				throw new InvalidParameterException(String.format("Timeout must > 0 . Table %s.", schema.getTableName()));
			}
			this.timeout = timeout;
			return this;
		}

		public Builder setSortKeys(SortKeys sortKeys) {
			this.sortKeys = sortKeys;
			return this;
		}

		public Scan build() {
			return new Scan(schema, filterList, selectedColumns, fetchSize, timeout, sortKeys);
		}
	}
}
