/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValues;
import com.alibaba.hologres.client.impl.util.StatementBuilderUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.expression.Expression;
import com.alibaba.hologres.client.model.expression.RecordWithExpression;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Tuple;
import com.alibaba.hologres.client.utils.Tuple5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** insert into xxx select unnest(?),unnest(?),... 的构造类. */
public class UnnestUpsertStatementBuilder extends UpsertStatementBuilder {
    public static final Logger LOGGER = LoggerFactory.getLogger(UnnestUpsertStatementBuilder.class);

    public UnnestUpsertStatementBuilder(HoloConfig config) {
        super(config);
    }

    /** 表+record.isSet的列（BitSet） -> Sql语句的映射. */
    static class SqlCache<T, R> {
        Map<
                        Tuple5<
                                TableSchema,
                                TableName,
                                OnConflictAction,
                                CheckAndPutCondition,
                                Expression>,
                        Map<T, R>>
                cacheMap = new HashMap<>();

        int size = 0;

        public R computeIfAbsent(
                Tuple5<TableSchema, TableName, OnConflictAction, CheckAndPutCondition, Expression>
                        tuple,
                T t,
                BiFunction<
                                Tuple5<
                                        TableSchema,
                                        TableName,
                                        OnConflictAction,
                                        CheckAndPutCondition,
                                        Expression>,
                                T,
                                R>
                        b) {
            Map<T, R> subMap = cacheMap.computeIfAbsent(tuple, (s) -> new HashMap<>());
            return subMap.computeIfAbsent(
                    t,
                    (bs) -> {
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

    /** insert类. */
    class InsertSql {
        public String sql;
        public boolean isUnnest;
    }

    SqlCache<Tuple<BitSet, BitSet>, InsertSql> insertCache = new SqlCache<>();
    boolean first = true;

    enum NotSupportReasonCode {
        UNNEST_NOT_SUPPORT_PARTITION_TABLE,
        UNNEST_NOT_SUPPORT_INSERT_ONLY,
        UNNEST_NOT_SUPPORT_TYPE
    }

    Set<Tuple<NotSupportReasonCode, Tuple<TableSchema, Column>>> reasonSet = new HashSet<>();

    private boolean isSupportUnnest(TableSchema schema, Tuple<BitSet, BitSet> columnSet) {
        if (columnSet.r.cardinality() > 0) {
            LOGGER.warn(
                    "Not support unnest，because Put for table {} contain insertOnlyColumn {} ",
                    schema.getTableNameObj().getFullName(),
                    columnSet.r);
            return false;
        }
        int index = -1;
        for (Column column : schema.getColumnSchema()) {
            ++index;
            if (columnSet.l.get(index)) {
                if (!StatementBuilderUtil.isTypeSupportForUnnest(
                        column.getType(), column.getTypeName())) {
                    LOGGER.warn(
                            "Not support unnest，because Put for table {} contain unsupported column {}({}) ",
                            schema.getTableNameObj().getFullName(),
                            column.getName(),
                            column.getTypeName());
                    return false;
                }
            }
        }
        return true;
    }

    private InsertSql buildInsertSql(
            Tuple5<TableSchema, TableName, OnConflictAction, CheckAndPutCondition, Expression>
                    tuple,
            Tuple<BitSet, BitSet> input) {
        TableSchema schema = tuple.f0;
        TableName tableName = tuple.f1;
        OnConflictAction onConflictAction = tuple.f2;
        CheckAndPutCondition checkAndPutCondition = tuple.f3;
        Expression expression = tuple.f4;
        BitSet set = input.l;
        BitSet onlyInsertSet = input.r;
        boolean isUnnest = false;
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName.getFullName());

        // 无论是与表中已有数据还是常量比较，都需要as重命名为old
        if (checkAndPutCondition != null || expression != null) {
            sb.append(" as old ");
        }

        sb.append("(");
        first = true;
        set.stream()
                .forEach(
                        (index) -> {
                            if (!first) {
                                sb.append(",");
                            }
                            first = false;
                            sb.append(
                                    IdentifierUtil.quoteIdentifier(
                                            schema.getColumn(index).getName(), true));
                        });
        sb.append(")");

        if (isSupportUnnest(schema, input)) {
            isUnnest = true;
            sb.append(" select ");
            first = true;
            set.stream()
                    .forEach(
                            (index) -> {
                                if (!first) {
                                    sb.append(",");
                                }
                                first = false;
                                Column column = schema.getColumn(index);
                                sb.append("unnest(?::")
                                        .append(
                                                StatementBuilderUtil.getRealTypeName(
                                                        column.getType(), column.getTypeName()))
                                        .append("[])");
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
            set.stream()
                    .forEach(
                            (index) -> {
                                if (!first) {
                                    sb.append(",");
                                }
                                first = false;
                                sb.append("?");
                                Column column = schema.getColumn(index);
                                if (Types.BIT == column.getType()
                                        && "bit".equals(column.getTypeName())) {
                                    sb.append("::bit(").append(column.getPrecision()).append(")");
                                } else if (Types.OTHER == column.getType()
                                        && "varbit".equals(column.getTypeName())) {
                                    sb.append("::bit varying(")
                                            .append(column.getPrecision())
                                            .append(")");
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
                sb.append(
                        IdentifierUtil.quoteIdentifier(
                                schema.getColumnSchema()[index].getName(), true));
            }
            sb.append(") do ");
            if (OnConflictAction.INSERT_OR_IGNORE == onConflictAction) {
                sb.append("nothing");
            } else if (expression != null && expression.hasConflictUpdateSet()) {
                sb.append("update set ");
                sb.append(expression.conflictUpdateSet);
            } else {
                sb.append("update set ");
                first = true;
                set.stream()
                        .forEach(
                                (index) -> {
                                    if (!onlyInsertSet.get(index)) {
                                        if (!first) {
                                            sb.append(",");
                                        }
                                        first = false;
                                        String columnName =
                                                IdentifierUtil.quoteIdentifier(
                                                        schema.getColumnSchema()[index].getName(),
                                                        true);
                                        sb.append(columnName)
                                                .append("=excluded.")
                                                .append(columnName);
                                    }
                                });
            }
            if (checkAndPutCondition != null) {
                sb.append(" where");
                sb.append(buildCheckAndPutPattern(checkAndPutCondition));
            } else if (expression != null && expression.hasConflictWhere()) {
                sb.append(" where ");
                sb.append(expression.conflictWhere);
            }
        }

        String sql = sb.toString();

        LOGGER.debug("new sql:{}", sql);
        InsertSql insertSql = new InsertSql();
        insertSql.isUnnest = isUnnest;
        insertSql.sql = sql;
        return insertSql;
    }

    private static final HoloVersion SUPPORT_VERSION = new HoloVersion(1, 1, 38);

    private boolean isVersionSupport(HoloVersion version) {
        if (version.compareTo(SUPPORT_VERSION) < 0) {
            return false;
        }
        return true;
    }

    @Override
    protected void buildInsertStatement(
            Connection conn,
            HoloVersion version,
            TableSchema schema,
            TableName tableName,
            Tuple<BitSet, BitSet> columnSet,
            List<Record> recordList,
            List<PreparedStatementWithBatchInfo> list,
            OnConflictAction onConflictAction)
            throws SQLException {
        // 版本不符合直接采用老链路工作
        if (!isVersionSupport(version)) {
            super.buildInsertStatement(
                    conn,
                    version,
                    schema,
                    tableName,
                    columnSet,
                    recordList,
                    list,
                    onConflictAction);
            return;
        }
        Record re = recordList.get(0);
        CheckAndPutCondition checkAndPutCondition = null;
        if (re instanceof CheckAndPutRecord) {
            checkAndPutCondition = ((CheckAndPutRecord) re).getCheckAndPutCondition();
        }
        Expression expression = null;
        if (re instanceof RecordWithExpression) {
            expression = ((RecordWithExpression) re).getExpression();
            if (!Expression.isVersionSupport(version, Put.MutationType.INSERT)) {
                throw new SQLException(
                        String.format(
                                "Insert RecordWithExpression is supported after holo version %s",
                                Expression.INSERT_SUPPORT_VERSION));
            }
        }

        InsertSql insertSql =
                insertCache.computeIfAbsent(
                        new Tuple5<>(
                                schema,
                                tableName,
                                onConflictAction,
                                checkAndPutCondition,
                                expression),
                        columnSet,
                        this::buildInsertSql);
        PreparedStatement currentPs = null;
        try {
            currentPs = conn.prepareStatement(insertSql.sql);

            // 解析出来的sql必须满足unnest才走新链路
            if (insertSql.isUnnest) {
                long totalBytes =
                        recordList.stream().collect(Collectors.summingLong(r -> r.getByteSize()));
                int rows = recordList.size();
                int stepRows = 0;
                if (config.getMaxBytesPerSql() > 0L) {
                    long avg = Math.max(totalBytes / recordList.size(), 1);
                    stepRows =
                            (int)
                                    Math.min(
                                            config.getMaxBytesPerSql() / avg,
                                            config.getMaxRowsPerSql());
                }
                stepRows = Math.min(recordList.size(), Math.max(stepRows, 0));

                if (stepRows != 0) {
                    rows = recordList.size() % stepRows + stepRows;
                }
                boolean isInit = false;
                boolean isFirstBatch = true;
                JdbcColumnValues[] arrayList = new JdbcColumnValues[columnSet.l.cardinality()];
                // 准备一个batch rows行的存储对象
                StatementBuilderUtil.prepareColumnValues(
                        conn, rows, columnSet.l, schema, this.config, arrayList);
                // 表示在一个batch中第N行
                int row = 0;

                int batchCount = 0;
                for (Record record : recordList) {
                    // 没有初始化batch就初始化
                    if (!isInit) {
                        if (isFirstBatch) {
                            isFirstBatch = false;
                        } else {
                            rows = stepRows;
                        }
                        // 准备一个batch rows行的存储对象
                        StatementBuilderUtil.prepareColumnValues(
                                conn, rows, columnSet.l, schema, this.config, arrayList);
                        ++batchCount;
                        isInit = true;
                    }
                    // 第几列
                    int arrayIndex = -1;
                    IntStream columnStream = columnSet.l.stream();
                    for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
                        int index = it.next();
                        ++arrayIndex;
                        arrayList[arrayIndex].set(row, record.getObject(index));
                    }
                    ++row;
                    // 如果一个batch攒够了
                    if (row == rows) {
                        // 收尾当前batch
                        {
                            arrayIndex = -1;
                            columnStream = columnSet.l.stream();
                            for (PrimitiveIterator.OfInt it = columnStream.iterator();
                                    it.hasNext(); ) {
                                ++arrayIndex;
                                int index = it.next();
                                Column column = schema.getColumn(index);

                                Array array =
                                        conn.createArrayOf(
                                                StatementBuilderUtil.getRealTypeName(
                                                        column.getType(), column.getTypeName()),
                                                arrayList[arrayIndex].getArray());
                                currentPs.setArray(arrayIndex + 1, array);
                            }
                            isInit = false;
                        }
                        // 如果一个PreparedStatement不止一个batch，那么要用addBatch来处理
                        if (rows < recordList.size()) {
                            currentPs.addBatch();
                        }
                        row = 0;
                    }
                }
                PreparedStatementWithBatchInfo preparedStatementWithBatchInfo =
                        new PreparedStatementWithBatchInfo(
                                currentPs, rows < recordList.size(), Put.MutationType.INSERT);
                preparedStatementWithBatchInfo.setByteSize(totalBytes);
                preparedStatementWithBatchInfo.setBatchCount(batchCount);
                list.add(preparedStatementWithBatchInfo);
            } else {
                super.buildInsertStatement(
                        conn,
                        version,
                        schema,
                        tableName,
                        columnSet,
                        recordList,
                        list,
                        onConflictAction);
            }
        } catch (SQLException e) {
            if (null != currentPs) {
                try {
                    currentPs.close();
                } catch (SQLException e1) {

                }
            }
            throw e;
        } finally {
            if (insertCache.getSize() > 500) {
                insertCache.clear();
            }
        }
    }
}
