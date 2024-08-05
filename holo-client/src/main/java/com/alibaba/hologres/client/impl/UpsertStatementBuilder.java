/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.hologres.client.type.PGroaringbitmap;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Tuple;
import com.alibaba.hologres.client.utils.Tuple3;
import com.alibaba.hologres.client.utils.Tuple4;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * build upsert statement.
 */
public class UpsertStatementBuilder {
	public static final Logger LOGGER = LoggerFactory.getLogger(UpsertStatementBuilder.class);
	protected static final String DELIMITER_OR = " OR ";
	protected static final String DELIMITER_DOT = ", ";
	protected HoloConfig config;
	boolean enableDefaultValue;
	String defaultTimeStampText;

	boolean inputNumberAsEpochMsForDatetimeColumn;
	boolean inputStringAsEpochMsForDatetimeColumn;
	boolean removeU0000InTextColumnValue;

	public UpsertStatementBuilder(HoloConfig config) {
		this.config = config;
		this.enableDefaultValue = config.isEnableDefaultForNotNullColumn();
		this.defaultTimeStampText = config.getDefaultTimestampText();
		this.inputNumberAsEpochMsForDatetimeColumn = config.isInputNumberAsEpochMsForDatetimeColumn();
		this.inputStringAsEpochMsForDatetimeColumn = config.isInputStringAsEpochMsForDatetimeColumn();
		this.removeU0000InTextColumnValue = config.isRemoveU0000InTextColumnValue();
	}

	/**
	 * 表+record.isSet的列（BitSet） -> Sql语句的映射.
	 */
	static class SqlCache<T> {
		Map<Tuple4<TableSchema, TableName, WriteMode, CheckAndPutCondition>, Map<T, SqlTemplate>> cacheMap = new HashMap<>();

		int size = 0;

		public SqlTemplate computeIfAbsent(Tuple4<TableSchema, TableName, WriteMode, CheckAndPutCondition> tuple, T t, BiFunction<Tuple4<TableSchema, TableName, WriteMode, CheckAndPutCondition>, T, SqlTemplate> b) {
			Map<T, SqlTemplate> subMap = cacheMap.computeIfAbsent(tuple, (s) -> new HashMap<>());
			return subMap.computeIfAbsent(t, (bs) -> {
				++size;
				return b.apply(tuple, bs);
			});
		}

		public int getSize() {
			return size;
		}

		public void clear() {
			cacheMap.clear();
		}
	}

	SqlCache<Tuple<BitSet, BitSet>> insertCache = new SqlCache<>();
	Map<Tuple3<TableSchema, TableName, CheckAndPutCondition>, SqlTemplate> deleteCache = new HashMap<>();
	boolean first = true;

	private SqlTemplate buildDeleteSqlTemplate(Tuple3<TableSchema, TableName, CheckAndPutCondition> tuple) {
		TableSchema schema = tuple.l;
		TableName tableName = tuple.m;
		CheckAndPutCondition checkAndPutCondition = tuple.r;
		first = true;
		StringBuilder sb = new StringBuilder();
		sb.append("delete from ").append(tableName.getFullName());
		sb.append(" where ");
		String header = sb.toString();
		sb.setLength(0);
		sb.append("(");
		for (int index : schema.getKeyIndex()) {
			if (!first) {
				sb.append(" and ");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumnSchema()[index].getName(), true)).append("=?");
		}
		if (checkAndPutCondition != null) {
			sb.append(" and");
			sb.append(buildCheckAndDeletePattern(checkAndPutCondition));
		}
		sb.append(")");
		String rowText = sb.toString();
		int maxLevel = 32 - Integer.numberOfLeadingZeros(Short.MAX_VALUE / schema.getKeyIndex().length) - 1;

		SqlTemplate sqlTemplate = new SqlTemplate(header, null, rowText, DELIMITER_OR, maxLevel);
		LOGGER.debug("new sql:{}", sqlTemplate.getSql(1));
		return sqlTemplate;
	}

	/**
	 * 对delete语句， check and put 方式为`delete from table where (pk = ? and checkColumn < ?) or (pk = ? and checkColumn < ?);`
	 */
	protected String buildCheckAndDeletePattern(CheckAndPutCondition checkAndPutCondition) {
		Column checkColumn = checkAndPutCondition.getCheckColumn();
		CheckCompareOp checkOp = checkAndPutCondition.getCheckOp();
		Object checkValue = checkAndPutCondition.getCheckValue();
		StringBuilder sb = new StringBuilder();

		String oldColumnPattern = coalesceNullValue(checkAndPutCondition, IdentifierUtil.quoteIdentifier(checkAndPutCondition.getCheckColumn().getName(), true));
		if (checkAndPutCondition.isOldValueCheckWithNull()) {
			// 已有数据与null做比较：old column is null
			sb.append(" ").append(oldColumnPattern).append(" ").append(checkOp.getOperatorString());
			return sb.toString();
		}
		if (checkAndPutCondition.isNewValueCheckWithOldValue()) {
			// 准备删除的数据与已有数据做比较： new column checkOp old column
			sb.append(" ?");
		} else {
			// 常量与已有数据做比较：checkValue checkOp old column
			sb.append(" '").append(checkValue).append("'::").append(checkColumn.getTypeName());
		}
		sb.append(" ").append(checkOp.getOperatorString()).append(" ").append(oldColumnPattern);
		return sb.toString();
	}

	private SqlTemplate buildInsertSql(Tuple4<TableSchema, TableName, WriteMode, CheckAndPutCondition> tuple, Tuple<BitSet, BitSet> input) {
		TableSchema schema = tuple.f0;
		TableName tableName = tuple.f1;
		WriteMode mode = tuple.f2;
		CheckAndPutCondition checkAndPutCondition = tuple.f3;
		BitSet set = input.l;
		BitSet onlyInsertSet = input.r;
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(tableName.getFullName());

		// 无论是与表中已有数据还是常量比较，都需要as重命名为old
		if (checkAndPutCondition != null) {
			sb.append(" as old ");
		}

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

		sb.append(" values ");
		String header = sb.toString();
		sb.setLength(0);
		sb.append("(");
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
		String rowText = sb.toString();
		sb.setLength(0);

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
			if (checkAndPutCondition != null) {
				sb.append(" where");
				sb.append(buildCheckAndPutPattern(checkAndPutCondition));
			}
		}

		String tail = sb.toString();

		int maxLevel = 32 - Integer.numberOfLeadingZeros(Short.MAX_VALUE / set.cardinality()) - 1;
		SqlTemplate sqlTemplate = new SqlTemplate(header, tail, rowText, DELIMITER_DOT, maxLevel);
		LOGGER.debug("new sql:{}", sqlTemplate.getSql(0));
		return sqlTemplate;
	}

	protected String buildCheckAndPutPattern(CheckAndPutCondition checkAndPutCondition) {
		Column checkColumn = checkAndPutCondition.getCheckColumn();
		CheckCompareOp checkOp = checkAndPutCondition.getCheckOp();
		Object checkValue = checkAndPutCondition.getCheckValue();
		StringBuilder sb = new StringBuilder();

		String oldColumnPattern = coalesceNullValue(checkAndPutCondition, getCheckColumnNameAlias(checkAndPutCondition, /*old*/true));
		String newColumnPattern = " " + getCheckColumnNameAlias(checkAndPutCondition, /*old*/false);

		if (checkAndPutCondition.isOldValueCheckWithNull()) {
			// 已有数据与null做比较：old column is null
			sb.append(" ").append(oldColumnPattern).append(" ").append(checkOp.getOperatorString());
			return sb.toString();
		}
		if (checkAndPutCondition.isNewValueCheckWithOldValue()) {
			// 准备插入的数据与已有数据做比较：new column checkOp old column
			sb.append(newColumnPattern);
		} else {
			// 常量与已有数据做比较：checkValue checkOp old column
			sb.append(" '").append(checkValue).append("'::").append(checkColumn.getTypeName());
		}
		sb.append(" ").append(checkOp.getOperatorString()).append(" ").append(oldColumnPattern);
		return sb.toString();
	}

	private static String getCheckColumnNameAlias(CheckAndPutCondition checkAndPutCondition, boolean old) {
		return (old ? "old." : "excluded.") + IdentifierUtil.quoteIdentifier(checkAndPutCondition.getCheckColumn().getName(), true);
	}

	private static String coalesceNullValue(CheckAndPutCondition checkAndPutCondition, String name) {
		StringBuilder sb = new StringBuilder();
		if (checkAndPutCondition.getNullValue() != null) {
			sb.append("coalesce(")
					.append(name)
					.append(", '").append(checkAndPutCondition.getNullValue()).append("'::").append(checkAndPutCondition.getCheckColumn().getTypeName()).append(")");
		} else {
			sb.append(name);
		}
		return sb.toString();
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

	protected void fillDefaultValue(Record record, Column column, int i) {

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
					case Types.REAL:
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
					case Types.REAL:
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
	protected void fillNotSetValue(Record record, Column column, int i) {
		if (!record.isSet(i)) {
			if (column.isSerial()) {
				return;
			}
			record.setObject(i, null);
		}
	}

	protected void handleArrayColumn(Connection conn, Record record, Column column, int index) throws SQLException {
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

	public void prepareRecord(Connection conn, Record record, WriteMode mode) throws SQLException {
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

	private String removeU0000(final String in) {
		if (removeU0000InTextColumnValue && in != null && in.contains("\u0000")) {
			return in.replaceAll("\u0000", "");
		} else {
			return in;
		}
	}

	private static final String MYSQL_0000 = "0000-00-00 00:00:00";

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
			case Types.LONGNVARCHAR:
			case Types.VARCHAR:
			case Types.CHAR:
				if (obj == null) {
					ps.setNull(index, column.getType());
				} else {
					ps.setObject(index, removeU0000(obj.toString()), column.getType());
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
			case Types.TIMESTAMP_WITH_TIMEZONE:
			case Types.TIMESTAMP:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new Timestamp(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new Timestamp(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new Timestamp(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new Timestamp(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			case Types.DATE:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new java.sql.Date(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new java.sql.Date(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new java.sql.Date(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new java.sql.Date(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIME:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new Time(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new Time(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new Time(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new Time(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			default:
				ps.setObject(index, obj, column.getType());
		}
	}

	private int fillPreparedStatementForInsert(PreparedStatement ps, int psIndex, Record record) throws SQLException {
		IntStream columnStream = record.getBitSet().stream();
		for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
			int index = it.next();
			Column column = record.getSchema().getColumn(index);
			++psIndex;
			fillPreparedStatement(ps, psIndex, record.getObject(index), column);
		}
		return psIndex;
	}

	protected void buildInsertStatement(Connection conn, HoloVersion version, TableSchema schema, TableName tableName, Tuple<BitSet, BitSet> columnSet, List<Record> recordList, List<PreparedStatementWithBatchInfo> list, WriteMode mode) throws SQLException {
		if (recordList.size() == 0) {
			return;
		}
		Record r = recordList.get(0);
		CheckAndPutCondition checkAndPutCondition = null;
		if (r instanceof CheckAndPutRecord) {
			checkAndPutCondition = ((CheckAndPutRecord) r).getCheckAndPutCondition();
		}
		SqlTemplate sql = insertCache.computeIfAbsent(new Tuple4<>(schema, tableName, mode, checkAndPutCondition), columnSet, this::buildInsertSql);
		fillPreparedStatement(conn, sql, list, recordList, Put.MutationType.INSERT, this::fillPreparedStatementForInsert);
	}

	private void fillPreparedStatement(Connection conn, SqlTemplate sqlTemplate, List<PreparedStatementWithBatchInfo> list, List<Record> recordList, Put.MutationType type, FillPreparedStatementFunc func) throws SQLException {
		int maxValueBlocks = 1 << sqlTemplate.maxLevel;
		int unprocessedBatchCount = recordList.size();
		final int fullValueBlocksCount = unprocessedBatchCount / maxValueBlocks;
		int remainFullValueBlocksCount = fullValueBlocksCount;
		boolean first = true;
		int currentLevel = 0;
		int rows = 0;
		int psIndex = 0;
		PreparedStatementWithBatchInfo ps = null;
		boolean batchMode = false;
		long byteSize = 0L;
		int batchCount = 0;
		for (Record record : recordList) {
			if (first) {
				if (remainFullValueBlocksCount > 0) {
					batchMode = fullValueBlocksCount > 1 ? true : false;
					currentLevel = sqlTemplate.getMaxLevel();
					if (ps == null) {
						ps = new PreparedStatementWithBatchInfo(conn.prepareStatement(sqlTemplate.getSql(currentLevel)), fullValueBlocksCount > 1, type);
						list.add(ps);
					}
					--remainFullValueBlocksCount;
				} else {
					if (ps != null) {
						ps.setByteSize(byteSize);
						ps.setBatchCount(batchCount);
						byteSize = 0L;
						batchCount = 0;
					}
					batchMode = false;
					currentLevel = 31 - Integer.numberOfLeadingZeros(unprocessedBatchCount);
					ps = new PreparedStatementWithBatchInfo(conn.prepareStatement(sqlTemplate.getSql(currentLevel)), false, type);
					list.add(ps);
				}
				first = false;
				rows = 1 << currentLevel;
				psIndex = 0;
				++batchCount;
			}
			--unprocessedBatchCount;
			if (rows > 0) {
				psIndex = func.apply(ps.l, psIndex, record);
				byteSize += record.getByteSize();
				--rows;
			}
			if (rows == 0) {
				first = true;
				if (batchMode) {
					ps.l.addBatch();
				}
			}
		}
		if (ps != null) {
			ps.setByteSize(byteSize);
			ps.setBatchCount(batchCount);
		}
	}

	private int fillPreparedStatementForDelete(PreparedStatement ps, int psIndex, Record record) throws SQLException {
		for (int index : record.getSchema().getKeyIndex()) {
			Column column = record.getSchema().getColumn(index);
			fillPreparedStatement(ps, ++psIndex, record.getObject(index), column);
		}
		if (record instanceof CheckAndPutRecord) {
			CheckAndPutCondition checkAndPutCondition = ((CheckAndPutRecord) record).getCheckAndPutCondition();
			// 已有数据和常量做比较，不需要传入参数
			if (checkAndPutCondition.isNewValueCheckWithOldValue()) {
				Column column = checkAndPutCondition.getCheckColumn();
				int index = record.getSchema().getColumnIndex(column.getName());
				fillPreparedStatement(ps, ++psIndex, record.getObject(index), column);
			}
		}
		return psIndex;
	}

	protected void buildDeleteStatement(Connection conn, HoloVersion version, TableSchema schema, TableName tableName, List<Record> recordList, List<PreparedStatementWithBatchInfo> list) throws SQLException {
		if (recordList.size() == 0) {
			return;
		}
		Record r = recordList.get(0);
		CheckAndPutCondition checkAndPutCondition = null;
		if (r instanceof CheckAndPutRecord) {
			checkAndPutCondition = ((CheckAndPutRecord) r).getCheckAndPutCondition();
		}
		SqlTemplate sql = deleteCache.computeIfAbsent(new Tuple3<>(schema, tableName, checkAndPutCondition), this::buildDeleteSqlTemplate);
		fillPreparedStatement(conn, sql, list, recordList, Put.MutationType.DELETE, this::fillPreparedStatementForDelete);
	}

	class SqlTemplate {
		private final String header;
		private final String tail;
		private final String rowText;
		private final String delimiter;
		private final int maxLevel;

		public SqlTemplate(String header, String tail, String rowText, String delimiter, int maxLevel) {
			this.header = header;
			this.tail = tail;
			this.rowText = rowText;
			this.delimiter = delimiter;
			this.maxLevel = maxLevel;
			sqls = new String[maxLevel + 1];
		}

		String[] sqls;

		public String getSql(int level) {
			if (level >= sqls.length) {
				throw new RuntimeException(this + " max level is " + sqls.length + ", but input level is " + level);
			}
			if (sqls[level] == null) {
				StringBuilder sb = new StringBuilder();
				if (header != null) {
					sb.append(header);
				}
				for (int i = 0; i < (1 << level); ++i) {
					if (i > 0) {
						sb.append(delimiter);
					}
					sb.append(rowText);
				}
				if (null != tail) {
					sb.append(tail);
				}
				sqls[level] = sb.toString();
			}
			return sqls[level];
		}

		public int getMaxLevel() {
			return maxLevel;
		}

		@Override
		public String toString() {
			return "SqlTemplate{" +
					"header='" + header + '\'' +
					", tail='" + tail + '\'' +
					", rowText='" + rowText + '\'' +
					", delimiter='" + delimiter + '\'' +
					'}';
		}

	}

	/**
	 * @param conn
	 * @param recordList 必须都是同一张表的！！！！
	 * @param mode
	 * @return
	 * @throws SQLException
	 */
	public List<PreparedStatementWithBatchInfo> buildStatements(Connection conn, HoloVersion version,
																TableSchema schema, TableName tableName, Collection<Record> recordList,
																WriteMode mode) throws
			SQLException {
		List<Record> deleteRecordList = new ArrayList<>();
		Map<Tuple<BitSet, BitSet>, List<Record>> insertRecordList = new HashMap<>();
		List<PreparedStatementWithBatchInfo> preparedStatementList = new ArrayList<>();
		try {
			for (Record record : recordList) {
				prepareRecord(conn, record, mode);
				switch (record.getType()) {
					case DELETE:
						deleteRecordList.add(record);
						break;
					case INSERT:
						insertRecordList.computeIfAbsent(new Tuple<>(record.getBitSet(), record.getOnlyInsertColumnSet()), t -> new ArrayList<>()).add(record);
						break;
					default:
						throw new SQLException("unsupported type:" + record.getType() + " for record:" + record);
				}
			}

			try {
				if (deleteRecordList.size() > 0) {
					buildDeleteStatement(conn, version, schema, tableName, deleteRecordList, preparedStatementList);
				}
				for (Map.Entry<Tuple<BitSet, BitSet>, List<Record>> entry : insertRecordList.entrySet()) {
					buildInsertStatement(conn, version, schema, tableName, entry.getKey(), entry.getValue(), preparedStatementList, mode);
				}
			} catch (SQLException e) {
				for (PreparedStatementWithBatchInfo psWithInfo : preparedStatementList) {
					PreparedStatement ps = psWithInfo.l;
					if (null != ps) {
						try {
							ps.close();
						} catch (SQLException e1) {

						}
					}

				}
				throw e;
			}
			return preparedStatementList;
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		} finally {
			if (insertCache.getSize() > 500) {
				insertCache.clear();
			}
			if (deleteCache.size() > 500) {
				deleteCache.clear();
			}
		}
	}
}

interface FillPreparedStatementFunc {
	int apply(PreparedStatement ps, int psIndex, Record record) throws SQLException;
}
