/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValues;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValuesBuilder;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.type.PGroaringbitmap;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Tuple;
import org.postgresql.jdbc.PgConnection;
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * insert into xxx select unnest(?),unnest(?),... 的构造类.
 */
public class UnnestUpsertStatementBuilder extends UpsertStatementBuilder {
	public static final Logger LOGGER = LoggerFactory.getLogger(UnnestUpsertStatementBuilder.class);
	boolean enableDefaultValue;
	String defaultTimeStampText;

	public UnnestUpsertStatementBuilder(HoloConfig config) {
		super(config);
		this.enableDefaultValue = config.isEnableDefaultForNotNullColumn();
		this.defaultTimeStampText = config.getDefaultTimestampText();
	}

	/**
	 * 表+record.isSet的列（BitSet） -> Sql语句的映射.
	 */
	static class SqlCache<T, R> {
		Map<TableSchema, Map<T, R>> cacheMap = new HashMap<>();

		int size = 0;

		public R computeIfAbsent(TableSchema tableSchema, T t, BiFunction<TableSchema, T, R> b) {
			Map<T, R> subMap = cacheMap.computeIfAbsent(tableSchema, (s) -> new HashMap<>());
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

	/**
	 * insert类.
	 */
	class InsertSql {
		public String sql;
		public boolean isUnnest;
	}

	SqlCache<Tuple<BitSet, BitSet>, InsertSql> insertCache = new SqlCache<>();
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

	enum NotSupportReasonCode {
		UNNEST_NOT_SUPPORT_PARTITION_TABLE,
		UNNEST_NOT_SUPPORT_INSERT_ONLY,
		UNNEST_NOT_SUPPORT_TYPE
	}

	Set<Tuple<NotSupportReasonCode, Tuple<TableSchema, Column>>> reasonSet = new HashSet<>();

	private boolean isSupportUnnest(TableSchema schema, Tuple<BitSet, BitSet> columnSet) {
		if (schema.getPartitionIndex() > -1) {
			LOGGER.warn("Not support unnest，because table {} is a partition table", schema.getTableNameObj().getFullName());
			return false;
		}
		if (columnSet.r.cardinality() > 0) {
			LOGGER.warn("Not support unnest，because Put for table {} contain insertOnlyColumn {} ", schema.getTableNameObj().getFullName(), columnSet.r);
			return false;
		}
		IntStream columnStream = columnSet.l.stream();
		int index = -1;
		for (Column column : schema.getColumnSchema()) {
			++index;
			if (columnSet.l.get(index)) {
				if (!isTypeSupportForUnnest(column.getType(), column.getTypeName())) {
					LOGGER.warn("Not support unnest，because Put for table {} contain unsupported column {}({}) ", schema.getTableNameObj().getFullName(), column.getName(), column.getTypeName());
					return false;
				}
			} else {
				if (column.getDefaultValue() != null) {
					LOGGER.warn("Not support unnest，because Put for table {} not insert column {} has default value {}", schema.getTableNameObj().getFullName(), column.getName(), column.getDefaultValue());
					return false;
				}
			}
		}
		return true;
	}

	private InsertSql buildInsertSql(TableSchema schema, Tuple<BitSet, BitSet> input) {
		BitSet set = input.l;
		BitSet onlyInsertSet = input.r;
		boolean isUnnest = false;
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

		if (isSupportUnnest(schema, input)) {
			isUnnest = true;
			sb.append(" select ");
			first = true;
			set.stream().forEach((index) -> {
				if (!first) {
					sb.append(",");
				}
				first = false;
				Column column = schema.getColumn(index);
				sb.append("unnest(?::").append(getRealTypeName(column.getType(), column.getTypeName())).append("[])");
				/*Column column = schema.getColumn(index);
				if (Types.BIT == column.getType() && "bit".equals(column.getTypeName())) {
					sb.append("::bit(").append(column.getPrecision()).append(")");
				} else if (Types.OTHER == column.getType() && "varbit".equals(column.getTypeName())) {
					sb.append("::bit varying(").append(column.getPrecision()).append(")");
				}*/
			});

		} else {

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
		}
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
		InsertSql insertSql = new InsertSql();
		insertSql.isUnnest = isUnnest;
		insertSql.sql = sql;
		return insertSql;
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
				if (record.getType() == Put.MutationType.INSERT && mode != WriteMode.INSERT_OR_UPDATE) {
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

	private static String getRealTypeName(int type, String typeName) {
		String ret = null;
		switch (type) {
			case Types.INTEGER:
				if ("serial".equals(typeName)) {
					ret = "int"; // 避免出现serial
				} else {
					ret = typeName;
				}
				break;
			case Types.BIGINT:
				if ("bigserial".equals(typeName)) {
					ret = "bigint"; // 避免出现bigserial
				} else {
					ret = typeName;
				}
				break;
			default:
				ret = typeName;
		}
		return ret;
	}

	private boolean isTypeSupportForUnnest(int type, String typeName) {
		switch (type) {
			case Types.BOOLEAN:
			case Types.INTEGER:
			case Types.SMALLINT:
			case Types.TINYINT:
			case Types.REAL:
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
			case Types.DATE:
			case Types.TIME_WITH_TIMEZONE:
			case Types.NUMERIC:
				return true;
			case Types.BIT:
				if ("bool".equals(typeName)) {
					return true;
				}
				return false;
			case Types.BIGINT:
				if ("oid".equals(typeName)) {
					return false;
				}
				return true;
			case Types.OTHER:
				if ("json".equals(typeName) || "jsonb".equals(typeName)) {
					return true;
				}
				return false;
			default:
				return false;
		}
	}

	private void prepareColumnValues(Connection conn, int rows, BitSet insertColumnBitSet, TableSchema schema, JdbcColumnValues[] arrayList) throws SQLException {
		int arrayIndex = -1;
		IntStream columnStream = insertColumnBitSet.stream();
		for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
			int index = it.next();
			Column column = schema.getColumn(index);
			++arrayIndex;
			arrayList[arrayIndex] = JdbcColumnValuesBuilder.build(conn.unwrap(PgConnection.class), rows, column.getType(), column.getTypeName(), config);
		}
	}

	private static final HoloVersion SUPPORT_VERSION = new HoloVersion(1, 1, 38);

	private boolean isVersionSupport(HoloVersion version) {
		if (version.compareTo(SUPPORT_VERSION) == -1) {
			return false;
		}
		return true;
	}

	@Override
	protected void buildInsertStatement(Connection conn, HoloVersion version, TableSchema schema, Tuple<BitSet, BitSet> columnSet, List<Record> recordList, List<PreparedStatementWithBatchInfo> list) throws SQLException {
		//版本不符合直接采用老链路工作
		if (!isVersionSupport(version)) {
			super.buildInsertStatement(conn, version, schema, columnSet, recordList, list);
			return;
		}
		InsertSql insertSql = insertCache.computeIfAbsent(schema, columnSet, this::buildInsertSql);
		PreparedStatement currentPs = null;
		try {
			currentPs = conn.prepareStatement(insertSql.sql);

			//解析出来的sql必须满足unnest才走新链路
			if (insertSql.isUnnest) {
				long totalBytes = recordList.stream().collect(Collectors.summingLong(r -> r.getByteSize()));
				int rows = recordList.size();
				int stepRows = 0;
				if (config.getMaxBytesPerSql() > 0L) {
					long avg = Math.max(totalBytes / recordList.size(), 1);
					stepRows = (int) Math.min(config.getMaxBytesPerSql() / avg, config.getMaxRowsPerSql());
				}
				stepRows = Math.min(recordList.size(), Math.max(stepRows, 0));

				if (stepRows != 0) {
					rows = recordList.size() % stepRows + stepRows;
				}
				boolean isInit = false;
				boolean isFirstBatch = true;
				JdbcColumnValues[] arrayList = new JdbcColumnValues[columnSet.l.cardinality()];
				// 准备一个batch rows行的存储对象
				prepareColumnValues(conn, rows, columnSet.l, schema, arrayList);
				// 表示在一个batch中第N行
				int row = 0;

				int batchCount = 0;
				for (Record record : recordList) {
					//没有初始化batch就初始化
					if (!isInit) {
						if (isFirstBatch) {
							isFirstBatch = false;
						} else {
							rows = stepRows;
						}
						// 准备一个batch rows行的存储对象
						prepareColumnValues(conn, rows, columnSet.l, schema, arrayList);
						++batchCount;
						isInit = true;
					}
					//第几列
					int arrayIndex = -1;
					IntStream columnStream = columnSet.l.stream();
					for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
						int index = it.next();
						++arrayIndex;
						arrayList[arrayIndex].set(row, record.getObject(index));
					}
					++row;
					//如果一个batch攒够了
					if (row == rows) {
						//收尾当前batch
						{
							arrayIndex = -1;
							columnStream = columnSet.l.stream();
							for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
								++arrayIndex;
								int index = it.next();
								Column column = schema.getColumn(index);

								Array array = conn.createArrayOf(getRealTypeName(column.getType(), column.getTypeName()), arrayList[arrayIndex].getArray());
								currentPs.setArray(arrayIndex + 1, array);
							}
							isInit = false;
						}
						//如果一个PreparedStatement不止一个batch，那么要用addBatch来处理
						if (rows < recordList.size()) {
							currentPs.addBatch();
						}
						row = 0;
					}
				}
				PreparedStatementWithBatchInfo preparedStatementWithBatchInfo = new PreparedStatementWithBatchInfo(currentPs, rows < recordList.size(), Put.MutationType.INSERT);
				preparedStatementWithBatchInfo.setByteSize(totalBytes);
				preparedStatementWithBatchInfo.setBatchCount(batchCount);
				list.add(preparedStatementWithBatchInfo);
			} else {
				super.buildInsertStatement(conn, version, schema, columnSet, recordList, list);
			}
		} catch (SQLException e) {
			if (null != currentPs) {
				try {
					currentPs.close();
				} catch (SQLException e1) {

				}
			}
			throw e;
		}
	}
}
