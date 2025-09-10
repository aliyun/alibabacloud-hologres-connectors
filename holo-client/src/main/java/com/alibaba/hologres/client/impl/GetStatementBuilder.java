package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.List;
import java.util.PrimitiveIterator;

/** select * from xxx where (key1=? and key2=?...) or (key1=? and key2=?)... 的构造类. */
public class GetStatementBuilder {
    public static final Logger LOGGER = LoggerFactory.getLogger(GetStatementBuilder.class);
    protected HoloConfig config;

    public GetStatementBuilder(HoloConfig config) {
        this.config = config;
    }

    public PreparedStatement buildStatements(
            Connection conn,
            HoloVersion version,
            TableSchema schema,
            TableName tableName,
            BitSet selectColumnMask,
            List<Get> recordList)
            throws SQLException {
        String sql = buildGetSql(schema, tableName, selectColumnMask, recordList.size());
        LOGGER.debug("Get sql:{}", sql);
        PreparedStatement ps = conn.prepareStatement(sql);
        fillPreparedStatement(ps, recordList);
        return ps;
    }

    private String buildGetSql(
            TableSchema schema, TableName tableName, BitSet selectColumnMask, int batchSize) {
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
        sb.append(" from ").append(tableName.getFullName()).append(" where ");
        for (int i = 0; i < batchSize; ++i) {
            if (i > 0) {
                sb.append(" or ");
            }
            first = true;

            sb.append("( ");
            for (String key : schema.getPrimaryKeys()) {
                if (!first) {
                    sb.append(" and ");
                }
                first = false;
                sb.append(IdentifierUtil.quoteIdentifier(key, true)).append("=?");
            }
            sb.append(" ) ");
        }
        return sb.toString();
    }

    private void fillPreparedStatement(PreparedStatement ps, List<Get> recordList)
            throws SQLException {
        int paramIndex = 0;
        for (Get get : recordList) {
            Record record = get.getRecord();
            for (int keyIndex : record.getKeyIndex()) {
                ps.setObject(
                        ++paramIndex,
                        record.getObject(keyIndex),
                        record.getSchema().getColumn(keyIndex).getType());
            }
        }
    }
}
