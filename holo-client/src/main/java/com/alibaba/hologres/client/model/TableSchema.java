/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.impl.binlog.BinlogLevel;
import com.alibaba.hologres.client.utils.IdentifierUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 表结构.
 */
public class TableSchema implements Serializable {
	/**
	 * 构造类.
	 */
	public static class Builder {
		TableSchema tableSchema;

		AtomicLong tableIdSerial = new AtomicLong(0L);

		public Builder() {
			tableSchema = new TableSchema("temp_" + tableIdSerial.incrementAndGet(), "temp");
		}

		public Builder(String tableId, String schemaVersion) {
			tableSchema = new TableSchema(tableId, schemaVersion);
		}

		List<Column> columns = new ArrayList<>();

		public Builder setColumns(List<Column> columns) {
			this.columns = columns;
			return this;
		}

		public Builder addColumn(Column column) {
			columns.add(column);
			return this;
		}

		public Builder addColumns(List<Column> columns) {
			this.columns.addAll(columns);
			return this;
		}

		public Builder setTableName(TableName tableName) {
			tableSchema.tableName = tableName;
			return this;
		}

		public void setComment(String comment) {
			tableSchema.comment = comment;
		}

		public Builder setDistributionKeys(String[] distributionKeys) {
			tableSchema.distributionKeys = distributionKeys;
			return this;
		}

		public void setDictionaryEncoding(String[] dictionaryEncoding) {
			tableSchema.dictionaryEncoding = dictionaryEncoding;
		}

		public void setBitmapIndexKey(String[] bitmapIndexKey) {
			tableSchema.bitmapIndexKey = bitmapIndexKey;
		}

		public void setLifecycle(Long lifecycle) {
			tableSchema.lifecycle = lifecycle;
		}

		public void setClusteringKey(String[] clusteringKey) {
			tableSchema.clusteringKey = clusteringKey;
		}

		public void setSegmentKey(String[] segmentKey) {
			tableSchema.segmentKey = segmentKey;
		}

		public void setOrientation(String orientation) {
			tableSchema.orientation = orientation;
		}

		public void setBinlogLevel(String binlogLevel) {
			if ("replica".equals(binlogLevel)) {
				tableSchema.binlogLevel = BinlogLevel.REPLICA;
			} else {
				tableSchema.binlogLevel = BinlogLevel.NONE;
			}
		}

		public void setNotExist(Boolean notExist) {
			tableSchema.isNotExist = notExist;
		}

		public void setSensitive(Boolean sensitive) {
			tableSchema.isSensitive = sensitive;
		}

		public Builder setPartitionColumnName(String partitionColumnName) {
			tableSchema.partitionInfo = partitionColumnName;
			tableSchema.partitionIndex = -2;
			return this;
		}

		public Builder setPartitionColumnIndex(int partitionColumnIndex) {
			tableSchema.partitionInfo = null;
			tableSchema.partitionIndex = partitionColumnIndex;
			return this;
		}

		public TableSchema build() {
			tableSchema.columns = columns.toArray(new Column[]{});
			return tableSchema;
		}
	}

	String tableId;
	String schemaVersion;
	TableName tableName;
	Column[] columns;

	//--------table_property---------------
	String[] distributionKeys;
	/**
	 * DICTIONARY_ENCODING 列名，多值用","分隔 字典编码列.
	 */
	private String[] dictionaryEncoding;

	/**
	 * BITMAP_INDEX_KEY 列名，多值用","分隔  位图列/比特编码列.
	 */
	private String[] bitmapIndexKey;

	/**
	 * 表生命周期.
	 */
	private Long lifecycle;

	/**
	 * 聚簇索引.
	 */
	private String[] clusteringKey;

	/**
	 * 分段键.
	 */
	private String[] segmentKey;
	String partitionInfo;

	/**
	 * ORIENTATION "column/row".
	 */
	private String orientation = "column";

	private String comment;

	/**
	 * 是否开启binlog.
	 */
	private BinlogLevel binlogLevel = BinlogLevel.NONE;


	//--------------misc---------------
	private Boolean isNotExist = true;

	private Boolean isSensitive = false;

	//--------caculated_property-------
	Map<String, Integer> columnNameToIndexMapping;
	Map<String, Integer> lowerColumnNameToIndexMapping;
	int partitionIndex = -2;
	String[] primaryKeys;
	int[] keyIndex;
	int[] distributionKeyIndex;
	Set<String> primaryKeySet;

	//---------Deprecated lazy load------------------
	String[] columnNames = null;
	int[] columnTypes = null;
	String[] typeNames = null;

	private TableSchema(String tableId, String schemaVersion) {
		this.tableId = tableId;
		this.schemaVersion = schemaVersion;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableSchema that = (TableSchema) o;
		return tableId.equals(that.tableId)
				&& schemaVersion.equals(that.schemaVersion);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableId, schemaVersion);
	}

	public void calculateProperties() {
		if (columnNameToIndexMapping == null) {
			columnNameToIndexMapping = new HashMap<>();
			List<String> primaryKeyNames = new ArrayList<>();
			for (int i = 0; i < columns.length; ++i) {
				Column column = columns[i];
				columnNameToIndexMapping.put(column.getName(), i);
				if (column.getPrimaryKey()) {
					primaryKeyNames.add(column.getName());
				}
			}
			// The JDBC spec says when you have duplicate columns names,
			// the first one should be returned. So load the map in
			// reverse order so the first ones will overwrite later ones.
			lowerColumnNameToIndexMapping = new HashMap<>();
			for (int i = columns.length - 1; i > -1; --i) {
				Column column = columns[i];
				lowerColumnNameToIndexMapping.put(column.getName().toLowerCase(Locale.US), i);
			}

			if (partitionIndex > -2) {
				if (partitionIndex == -1) {
					partitionInfo = null;
				} else {
					partitionInfo = columns[partitionIndex].getName();
				}
			} else {
				if (null != partitionInfo && partitionInfo.length() > 0) {
					partitionIndex = columnNameToIndexMapping.get(partitionInfo);
				} else {
					partitionIndex = -1;
				}
			}

			primaryKeys = primaryKeyNames.toArray(new String[]{});
			keyIndex = new int[primaryKeys.length];
			if (primaryKeys.length > 0) {
				primaryKeySet = new HashSet<>();
			}
			for (int i = 0; i < primaryKeys.length; ++i) {
				keyIndex[i] = columnNameToIndexMapping.get(primaryKeys[i]);
				if (primaryKeySet != null) {
					primaryKeySet.add(primaryKeys[i]);
				}
			}

			if (distributionKeys == null) {
				distributionKeys = new String[]{};
			}
			distributionKeyIndex = new int[distributionKeys.length];
			for (int i = 0; i < distributionKeyIndex.length; ++i) {
				distributionKeyIndex[i] = columnNameToIndexMapping.get(distributionKeys[i]);
			}

			String[] typeNamesTemp = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				typeNamesTemp[i] = columns[i].getTypeName();
			}
			this.typeNames = typeNamesTemp;

			int[] columnTypesTemp = new int[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				columnTypesTemp[i] = columns[i].getType();
			}
			this.columnTypes = columnTypesTemp;

			String[] columnNamesTemp = new String[columns.length];
			for (int i = 0; i < columns.length; ++i) {
				columnNamesTemp[i] = columns[i].getName();
			}
			this.columnNames = columnNamesTemp;

		}
	}

	public String getTableId() {
		return tableId;
	}

	public String getSchemaVersion() {
		return schemaVersion;
	}

	public int getPartitionIndex() {
		return partitionIndex;
	}

	public boolean isPartitionParentTable() {
		return partitionIndex > -1;
	}

	public TableName getTableNameObj() {
		return tableName;
	}

	public String getSchemaName() {
		return tableName.getSchemaName();
	}

	public String getTableName() {
		return tableName.getTableName();
	}

	public Column[] getColumnSchema() {
		return columns;
	}

	public Column getColumn(int index) {
		return columns[index];
	}

	public Integer getColumnIndex(String columnName) {
		return columnNameToIndexMapping.get(columnName);
	}

	public Integer getLowerColumnIndex(String lowerColumnName) {
		return lowerColumnNameToIndexMapping.get(lowerColumnName);
	}

	public String[] getPrimaryKeys() {
		return primaryKeys;
	}

	public int[] getKeyIndex() {
		return keyIndex;
	}

	public String[] getDistributionKeys() {
		return distributionKeys;
	}

	public int[] getDistributionKeyIndex() {
		return distributionKeyIndex;
	}

	public String[] getDictionaryEncoding() {
		return dictionaryEncoding;
	}

	public String[] getBitmapIndexKey() {
		return bitmapIndexKey;
	}

	public Long getLifecycle() {
		return lifecycle;
	}

	public String[] getClusteringKey() {
		return clusteringKey;
	}

	public String[] getSegmentKey() {
		return segmentKey;
	}

	public String getPartitionInfo() {
		return partitionInfo;
	}

	public String getOrientation() {
		return orientation;
	}

	public Boolean getNotExist() {
		return isNotExist;
	}

	public Boolean getSensitive() {
		return isSensitive;
	}

	public String getComment() {
		return comment;
	}

	public BinlogLevel getBinlogLevel() {
		return binlogLevel;
	}

	public boolean isPrimaryKey(String columnName) {
		if (primaryKeySet != null) {
			return primaryKeySet.contains(columnName);
		}
		return false;
	}

	/**
	 * don`t call this function.
	 */
	public void convertTableIdentifier() {
		if (!"temp".equals(schemaVersion)) {
			return;
		}
		for (Column hologresColumn : this.getColumnSchema()) {
			hologresColumn.setName(IdentifierUtil.quoteIdentifier(hologresColumn.getName(), this.getSensitive()));
		}

		this.bitmapIndexKey = (convertArrayIdentfier(this.getBitmapIndexKey()));
		this.clusteringKey = (convertArrayIdentfier(this.getClusteringKey()));
		this.dictionaryEncoding = (convertArrayIdentfier(this.getDictionaryEncoding()));
		this.segmentKey = (convertArrayIdentfier(this.getSegmentKey()));
		this.distributionKeys = (convertArrayIdentfier(this.getDistributionKeys()));
	}

	private String[] convertArrayIdentfier(String[] array) {
		if (array != null) {
			for (int i = 0; i < array.length; i++) {
				array[i] = IdentifierUtil.quoteIdentifier(array[i], this.getSensitive());
			}
		}
		return array;
	}

	@Override
	public String toString() {
		return "TableSchema{" +
				"tableName=" + tableName +
				", columnNames=" + Arrays.toString(columnNames) +
				'}';
	}
}
