package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.handler.jdbc.JdbcColumnValues;
import com.alibaba.hologres.client.impl.util.StatementBuilderUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

/** select * from xxx where (key1, key2, ...) in (select unnest(?), unnest(?),...)的构造类. */
public class UnnestGetStatementBuilder extends GetStatementBuilder {
    public static final Logger LOGGER = LoggerFactory.getLogger(UnnestGetStatementBuilder.class);

    public UnnestGetStatementBuilder(HoloConfig config) {
        super(config);
    }

    private static final HoloVersion SUPPORT_VERSION = new HoloVersion(3, 0, 16);

    private boolean isVersionSupport(HoloVersion version) {
        if (version.compareTo(SUPPORT_VERSION) < 0) {
            return false;
        }
        return true;
    }

    private boolean isSupportUnnest(HoloVersion version, TableSchema schema) {
        if (!isVersionSupport(version)) {
            return false;
        }
        int[] keyIndex = schema.getKeyIndex();
        for (int index : keyIndex) {
            Column col = schema.getColumn(index);
            if (!StatementBuilderUtil.isTypeSupportForUnnest(col.getType(), col.getTypeName())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public PreparedStatement buildStatements(
            Connection conn,
            HoloVersion version,
            TableSchema schema,
            TableName tableName,
            BitSet selectColumnMask,
            List<Get> recordList)
            throws SQLException {
        if (!isSupportUnnest(version, schema)) {
            return super.buildStatements(
                    conn, version, schema, tableName, selectColumnMask, recordList);
        }
        String sql = buildGetSql(schema, tableName, selectColumnMask);
        LOGGER.debug("Get sql:{}", sql);
        PreparedStatement ps = conn.prepareStatement(sql);
        fillPreparedStatement(conn, ps, schema, recordList);
        return ps;
    }

    private String buildGetSql(TableSchema schema, TableName tableName, BitSet selectColumnMask) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (PrimitiveIterator.OfInt it = selectColumnMask.stream().iterator(); it.hasNext(); ) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(it.next()).getName(), true));
        }
        sb.append(" from ").append(tableName.getFullName()).append(" where (");

        first = true;
        for (String key : schema.getPrimaryKeys()) {
            if (!first) {
                sb.append(" , ");
            }
            first = false;
            sb.append(IdentifierUtil.quoteIdentifier(key, true));
        }
        sb.append(") in (select ");

        first = true;
        for (int keyIndex : schema.getKeyIndex()) {
            if (!first) {
                sb.append(" , ");
            }
            first = false;
            Column column = schema.getColumn(keyIndex);
            sb.append("unnest(?::")
                    .append(
                            StatementBuilderUtil.getRealTypeName(
                                    column.getType(), column.getTypeName()))
                    .append("[])");
        }
        sb.append(")");
        return sb.toString();
    }

    private void fillPreparedStatement(
            Connection conn, PreparedStatement ps, TableSchema schema, List<Get> recordList)
            throws SQLException {
        int batchSize = recordList.size();
        BitSet keySet = new BitSet();
        for (int keyIndex : schema.getKeyIndex()) {
            keySet.set(keyIndex);
        }
        JdbcColumnValues[] arrayList = new JdbcColumnValues[schema.getKeyIndex().length];
        StatementBuilderUtil.prepareColumnValues(
                conn, batchSize, keySet, schema, this.config, arrayList);

        int row = 0;
        for (Get get : recordList) {
            Record record = get.getRecord();
            int arrayIndex = -1;
            IntStream columnStream = keySet.stream();
            for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
                int index = it.next();
                ++arrayIndex;
                arrayList[arrayIndex].set(row, record.getObject(index));
            }
            row++;
        }

        int arrayIndex = -1;
        IntStream columnStream = keySet.stream();
        for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
            ++arrayIndex;
            int index = it.next();
            Column column = schema.getColumn(index);

            Array array =
                    conn.createArrayOf(
                            StatementBuilderUtil.getRealTypeName(
                                    column.getType(), column.getTypeName()),
                            arrayList[arrayIndex].getArray());
            ps.setArray(arrayIndex + 1, array);
        }
    }
}
