/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.WriteMode;
import org.postgresql.core.SqlCommandType;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.model.Column;
import org.postgresql.model.Partition;
import org.postgresql.model.TableName;
import org.postgresql.model.TableSchema;
import org.postgresql.roaringbitmap.PGroaringbitmap;
import org.postgresql.util.IdentifierUtil;
import org.postgresql.util.MetaUtil;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * build upsert statement.
 */
public class UpsertStatementBuilder {
	public static final Logger LOGGER = LoggerFactory.getLogger(UpsertStatementBuilder.class);
	WriteMode mode;
	boolean enableDefaultValue;
	String defaultTimeStampText;
	boolean dynamicPartition;
	boolean enableClientDynamicPartition;
	int refreshMetaTimeout;

	public UpsertStatementBuilder(HoloConfig config) {
		this.mode = config.getWriteMode();
		this.enableDefaultValue = config.isEnableDefaultForNotNullColumn();
		this.defaultTimeStampText = config.getDefaultTimestampText();
		this.dynamicPartition = config.isDynamicPartition();
		this.enableClientDynamicPartition = config.isEnableClientDynamicPartition();
		this.refreshMetaTimeout = config.getRefreshMetaTimeout();
	}

	/**
	 * 表+record.isSet的列（BitSet） -> Sql语句的映射.
	 */
	static class SqlCache<T> {
		Map<TableSchema, Map<T, String>> cacheMap = new HashMap<>();

		int size = 0;

		public String computeIfAbsent(TableSchema tableSchema, T t, BiFunction<TableSchema, T, String> b) {
			Map<T, String> subMap = cacheMap.computeIfAbsent(tableSchema, (s) -> new HashMap<>());
			return subMap.computeIfAbsent(t, (bs) -> {
				++size;
				return b.apply(tableSchema, bs);
			});
		}

		public int getSize() {
			return size;
		}

		public void clear() {
			cacheMap.clear();
		}
	}

	class Tuple<L, R> {
		public L l;
		public R r;

		public Tuple(L l, R r) {
			this.l = l;
			this.r = r;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Tuple<?, ?> tuple = (Tuple<?, ?>) o;
			return Objects.equals(l, tuple.l) &&
					Objects.equals(r, tuple.r);
		}

		@Override
		public int hashCode() {
			return Objects.hash(l, r);
		}
	}

	SqlCache<Tuple<BitSet, BitSet>> insertCache = new SqlCache<>();
	SqlCache<BitSet> updateCache = new SqlCache<>();
	Map<TableSchema, String> deleteCache = new HashMap<>();
	boolean first = true;

	private String buildDeleteSql(TableSchema schema) {
		String sql = deleteCache.get(schema);
		if (sql == null) {
			StringBuilder sb = new StringBuilder();
			sb.append("delete from ").append(schema.getTableNameObj().getFullName());
			sb.append(" where ");
			first = true;
			for (int index : schema.getKeyIndex()) {
				if (!first) {
					sb.append(" and ");
				}
				first = false;
				sb.append(IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName(), true)).append("=?");
			}
			sql = sb.toString();
			deleteCache.put(schema, sql);
		}
		return sql;
	}

	private String buildUpdateSql(TableSchema schema, BitSet set) {

		StringBuilder sb = new StringBuilder();
		sb.append("update ").append(schema.getTableNameObj().getFullName())
				.append(" set ");
		first = true;
		//从BitSet中剔除所有主键列，拼成 set a=?,b=?,... where
		set.stream().forEach((index) -> {
			boolean skip = false;
			for (int keyIndex : schema.getKeyIndex()) {
				if (index == keyIndex) {
					skip = true;
					break;
				}
			}
			if (skip) {
				return;
			}
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName(), true)).append("=?");
		});
		sb.append(" where ");
		first = true;
		//拼上主键条件 where pk0=? and pk1=? and ...
		for (int index : schema.getKeyIndex()) {
			if (!first) {
				sb.append(" and ");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName())).append("=?");
		}

		String sql = sb.toString();

		LOGGER.debug("new sql:{}", sql);
		return sql;
	}

	private String buildInsertSql(TableSchema schema, Tuple<BitSet, BitSet> input) {
		BitSet set = input.l;
		BitSet onlyInsertSet = input.r;
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(schema.getTableNameObj().getFullName());

		sb.append("(");
		first = true;
		set.stream().forEach((index) -> {
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(index).getName(), true));
		});
		sb.append(")");

		sb.append(" values (");
		first = true;
		set.stream().forEach((index) -> {
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append("?");
			Column column = schema.getColumn(index);
			if (Types.BIT == column.getType() && "bit".equals(column.getTypeName())) {
				sb.append("::bit(").append(column.getPrecision()).append(")");
			} else if (Types.OTHER == column.getType() && "varbit".equals(column.getTypeName())) {
				sb.append("::bit varying(").append(column.getPrecision()).append(")");
			}
		});
		sb.append(")");
		if (schema.getKeyIndex().length > 0) {
			sb.append(" on conflict (");
			first = true;
			for (int index : schema.getKeyIndex()) {
				if (!first) {
					sb.append(",");
				}
				first = false;
				sb.append(IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName(), true));
			}
			sb.append(") do ");
			if (WriteMode.INSERT_OR_IGNORE == mode) {
				sb.append("nothing");
			} else {
				sb.append("update set ");
				first = true;
				set.stream().forEach((index) -> {
					if (!onlyInsertSet.get(index)) {
						if (!first) {
							sb.append(",");
						}
						first = false;
						String columnName = IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName(), true);
						sb.append(columnName).append("=excluded.").append(columnName);
					}
				});
			}
		}
		String sql = sb.toString();

		LOGGER.debug("new sql:{}", sql);
		return sql;
	}

	/**
	 * 把default值解析为值和类型.
	 * 例:
	 * text default '-'  ,  ["-",null]
	 * timestamptz default '2021-12-12 12:12:12.123'::timestamp , ["2021-12-12 12:12:12.123","timestamp"]
	 *
	 * @param defaultValue 建表时设置的default值
	 * @return 2元组，值，[类型]
	 */
	private static String[] handleDefaultValue(String defaultValue) {
		String[] ret = defaultValue.split("::");
		if (ret.length == 1) {
			String[] temp = new String[2];
			temp[0] = ret[0];
			temp[1] = null;
			ret = temp;
		}
		if (ret[0].startsWith("'") && ret[0].endsWith("'") && ret[0].length() > 1) {
			ret[0] = ret[0].substring(1, ret[0].length() - 1);
		}
		return ret;
	}

	private static final int WARN_SKIP_COUNT = 10000;
	long warnCount = WARN_SKIP_COUNT;

	private void logWarnSeldom(String s, Object... obj) {
		if (++warnCount > WARN_SKIP_COUNT) {
			LOGGER.warn(s, obj);
			warnCount = 0;
		}
	}

	private void fillDefaultValue(Record record, Column column, int i) {

		//当列为空并且not null时，尝试在客户端填充default值
		if (record.getObject(i) == null && !column.getAllowNull()) {
			//对于serial列不处理default值
			if (column.isSerial()) {
				return;
			}
			if (column.getDefaultValue() != null) {
				String[] defaultValuePair = handleDefaultValue(String.valueOf(column.getDefaultValue()));
				String defaultValue = defaultValuePair[0];
				switch (column.getType()) {
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.SMALLINT:
						record.setObject(i, Long.parseLong(defaultValue));
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
						record.setObject(i, Double.parseDouble(defaultValue));
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						record.setObject(i, new BigDecimal(defaultValue));
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						record.setObject(i, Boolean.valueOf(defaultValue));
						break;
					case Types.CHAR:
					case Types.VARCHAR:
						record.setObject(i, defaultValue);
						break;
					case Types.TIMESTAMP:
					case Types.TIME_WITH_TIMEZONE:
					case Types.TIME:
					case Types.DATE:
						if ("now()".equalsIgnoreCase(defaultValue) || "current_timestamp".equalsIgnoreCase(defaultValue)) {
							record.setObject(i, new Date());
						} else {
							record.setObject(i, defaultValue);
						}
						break;
					default:
						logWarnSeldom("unsupported default type,{}({})", column.getType(), column.getTypeName());
				}
			} else if (enableDefaultValue) {
				switch (column.getType()) {
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.SMALLINT:
						record.setObject(i, 0L);
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
						record.setObject(i, 0D);
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						record.setObject(i, BigDecimal.ZERO);
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						record.setObject(i, false);
						break;
					case Types.CHAR:
					case Types.VARCHAR:
						record.setObject(i, "");
						break;
					case Types.TIMESTAMP:
					case Types.TIME_WITH_TIMEZONE:
					case Types.TIME:
					case Types.DATE:
						if (defaultTimeStampText == null) {
							record.setObject(i, new Date(0L));
						} else {
							record.setObject(i, defaultTimeStampText);
						}
						break;
					default:
						logWarnSeldom("unsupported default type,{}({})", column.getType(), column.getTypeName());
				}
			}
		}
	}

	/**
	 * 給所有非serial且没有set的列都set成null.
	 *
	 * @param record
	 */
	private void fillNotSetValue(Record record, Column column, int i) {
		if (!record.isSet(i)) {
			if (column.isSerial()) {
				return;
			}
			record.setObject(i, null);
		}
	}

	private void handleArrayColumn(Connection conn, Record record, Column column, int index) throws SQLException {
		Object obj = record.getObject(index);
		if (null != obj && obj instanceof List) {
			List<?> list = (List<?>) obj;
			Array array = conn.createArrayOf(column.getTypeName().substring(1), list.toArray());
			record.setObject(index, array);
		} else if (obj != null && obj instanceof Object[]) {
			Array array = conn.createArrayOf(column.getTypeName().substring(1), (Object[]) obj);
			record.setObject(index, array);
		}

	}

	public void prepareRecord(Connection conn, Record record) throws SQLException {
		try {
			for (int i = 0; i < record.getSize(); ++i) {
				Column column = record.getSchema().getColumn(i);
				if (record.getType() == SqlCommandType.INSERT && mode != WriteMode.INSERT_OR_UPDATE) {
					fillDefaultValue(record, column, i);
					fillNotSetValue(record, column, i);
				}
				if (column.getType() == Types.ARRAY) {
					handleArrayColumn(conn, record, column, i);
				}
			}
		} catch (Exception e) {
			throw new SQLException(PSQLState.INVALID_PARAMETER_VALUE.getState(), e);
		}
	}

	private void fillPreparedStatement(PreparedStatement ps, int index, Object obj, Column column) throws SQLException {
		switch (column.getType()) {
			case Types.OTHER:
				if (obj instanceof byte[] && "roaringbitmap".equalsIgnoreCase(column.getTypeName())) {
					PGroaringbitmap binaryObject = new PGroaringbitmap();
					byte[] bytes = (byte[]) obj;
					binaryObject.setByteValue(bytes, 0);
					ps.setObject(index, binaryObject, column.getType());
				} else if ("varbit".equals(column.getTypeName())) {
					ps.setString(index, obj == null ? null : String.valueOf(obj));
				} else {
					ps.setObject(index, obj, column.getType());
				}
				break;
			case Types.BIT:
				if ("bit".equals(column.getTypeName())) {
					if (obj instanceof Boolean) {
						ps.setString(index, (Boolean) obj ? "1" : "0");
					} else {
						ps.setString(index, obj == null ? null : String.valueOf(obj));
					}
				} else {
					ps.setObject(index, obj, column.getType());
				}
				break;
			default:
				ps.setObject(index, obj, column.getType());
		}
	}

	private void buildInsertStatement(Connection conn, Map<String, PreparedStatement> map, Record record) throws SQLException {
		TableSchema schema = record.getSchema();
		TableName tableName = schema.getTableNameObj();
		if (record.getSchema().getPartitionIndex() > -1 && enableClientDynamicPartition) {
			boolean isStr = Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType();
			String value = String.valueOf(record.getObject(schema.getPartitionIndex()));
			Partition partition = conn.unwrap(PgConnection.class).getMetaStore().partitionCache.get(schema.getTableNameObj()).get(value, (partitionValue) -> {
				if (refreshMetaTimeout > 0) {
					ConnectionUtil.refreshMeta(conn, refreshMetaTimeout);
				}
				return MetaUtil.getPartition(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
			});
			if (partition == null) {
				if (dynamicPartition) {
					try {
						partition = MetaUtil.retryCreatePartitionChildTable(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
					} catch (SQLException e) {
						partition = conn.unwrap(PgConnection.class).getMetaStore().partitionCache.get(tableName).get(value, (partitionValue) -> MetaUtil.getPartition(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr));
						if (partition == null) {
							throw new SQLException(e);
						}
					}
					if (partition != null) {
						TableSchema newSchema = conn.unwrap(PgConnection.class).getTableSchema(TableName.valueOf(IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true), IdentifierUtil.quoteIdentifier(partition.getTableName(), true)));
						record.changeToChildSchema(newSchema);
					} else {
						throw new SQLException("after create, partition child table is still not exists, tableName:" + tableName.getFullName() + ",partitionValue:" + value);
					}
				} else {
					throw new SQLException("partition child table is not exists, tableName:" + tableName.getFullName() + ",partitionValue:" + value);
				}
			} else {
				TableSchema newSchema = conn.unwrap(PgConnection.class).getTableSchema(TableName.valueOf(IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true), IdentifierUtil.quoteIdentifier(partition.getTableName(), true)));
				record.changeToChildSchema(newSchema);
			}
		}
		String sql = insertCache.computeIfAbsent(record.getSchema(), new Tuple<BitSet, BitSet>(record.getBitSet(), record.getOnlyInsertColumnSet()), this::buildInsertSql);
		PreparedStatement currentPs = map.get(sql);
		if (currentPs == null) {
			currentPs = conn.prepareStatement(sql);
			map.put(sql, currentPs);
		}
		int psIndex = 0;
		IntStream columnStream = record.getBitSet().stream();
		for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
			int index = it.next();
			Column column = record.getSchema().getColumn(index);
			++psIndex;
			fillPreparedStatement(currentPs, psIndex, record.getObject(index), column);
		}
		currentPs.addBatch();
	}

	private void buildUpdateStatement(Connection conn, Map<String, PreparedStatement> map, Record record) throws SQLException {
		String sql = updateCache.computeIfAbsent(record.getSchema(), record.getBitSet(), this::buildUpdateSql);
		PreparedStatement currentPs = map.get(sql);
		if (currentPs == null) {
			currentPs = conn.prepareStatement(sql);
			map.put(sql, currentPs);
		}
		int psIndex = 0;
		IntStream columnStream = record.getBitSet().stream();
		//填充update语句时，跳过所有的pk列，最后再填充pk列
		for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
			int index = it.next();
			Column column = record.getSchema().getColumn(index);
			if (record.getSchema().isPrimaryKey(column.getName())) {
				continue;
			}
			++psIndex;
			fillPreparedStatement(currentPs, psIndex, record.getObject(index), column);
		}
		for (int index : record.getSchema().getKeyIndex()) {
			Column column = record.getSchema().getColumn(index);
			fillPreparedStatement(currentPs, ++psIndex, record.getObject(index), column);
		}
		currentPs.addBatch();
	}

	private void buildDeleteStatement(Connection conn, Map<String, PreparedStatement> map, Record record) throws SQLException {
		TableSchema schema = record.getSchema();
		TableName tableName = schema.getTableNameObj();
		if (record.getSchema().getPartitionIndex() > -1) {
			boolean isStr = Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType();
			String value = String.valueOf(record.getObject(schema.getPartitionIndex()));
			Partition partition = conn.unwrap(PgConnection.class).getMetaStore().partitionCache.get(schema.getTableNameObj()).get(value, (partitionValue) -> {
				if (refreshMetaTimeout > 0) {
					ConnectionUtil.refreshMeta(conn, refreshMetaTimeout);
				}
				return MetaUtil.getPartition(conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
			});
			if (partition == null) {
				LOGGER.warn("delete from partition table {}, partition value={}, but partition child table is not exists, skip this record.{}", tableName.getFullName(), value, record);
				return;
			} else {
				TableSchema newSchema = conn.unwrap(PgConnection.class).getTableSchema(TableName.valueOf(IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true), IdentifierUtil.quoteIdentifier(partition.getTableName(), true)));
				record.changeToChildSchema(newSchema);
			}
		}
		String sql = deleteCache.computeIfAbsent(record.getSchema(), this::buildDeleteSql);
		PreparedStatement currentPs = map.get(sql);
		if (currentPs == null) {
			currentPs = conn.prepareStatement(sql);
			map.put(sql, currentPs);
		}
		int psIndex = 0;
		for (int index : record.getSchema().getKeyIndex()) {
			Column column = record.getSchema().getColumn(index);
			fillPreparedStatement(currentPs, ++psIndex, record.getObject(index), column);
		}
		currentPs.addBatch();
	}

	public PreparedStatement[] buildStatements(Connection conn, Collection<Record> recordList) throws SQLException {
		Map<String, PreparedStatement> preparedStatementMap = new HashMap<>();
		Map<String, PreparedStatement> deletePreparedStatementMap = new HashMap<>();
		try {
			for (Record record : recordList) {
				prepareRecord(conn, record);
				switch (record.getType()) {
					case DELETE:
						buildDeleteStatement(conn, deletePreparedStatementMap, record);
						break;
					case INSERT:
						buildInsertStatement(conn, preparedStatementMap, record);
						break;
					case UPDATE:
						buildUpdateStatement(conn, preparedStatementMap, record);
						break;
					default:
						throw new SQLException("unsupported type:" + record.getType() + " for record:" + record);
				}
			}
			int index = -1;
			PreparedStatement[] ret = new PreparedStatement[preparedStatementMap.size() + deletePreparedStatementMap.size()];
			for (PreparedStatement ps : deletePreparedStatementMap.values()) {
				ret[++index] = ps;
			}
			for (PreparedStatement ps : preparedStatementMap.values()) {
				ret[++index] = ps;
			}
			return ret;
		} catch (Exception e) {
			for (PreparedStatement ps : preparedStatementMap.values()) {
				try {
					ps.close();
				} catch (SQLException ignore) {
				}
			}
			for (PreparedStatement ps : deletePreparedStatementMap.values()) {
				try {
					ps.close();
				} catch (SQLException ignore) {
				}
			}
			throw new SQLException(e);
		} finally {
			if (insertCache.getSize() > 500) {
				insertCache.clear();
			}
			if (updateCache.getSize() > 500) {
				insertCache.clear();
			}
			if (deleteCache.size() > 500) {
				deleteCache.clear();
			}
		}
	}
}
