/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** 常用查询的封装. */
public class Command {

    public static int getShardCount(HoloClient client, TableSchema schema)
            throws HoloClientException {
        return get(
                client.sql(
                        conn -> {
                            int shardCount = -1;
                            try (PreparedStatement ps =
                                    conn.prepareStatement(
                                            "select g.property_value from hologres.hg_table_properties t,hologres.hg_table_group_properties g\n"
                                                    + "where t.property_key='table_group' and g.property_key='shard_count' and table_namespace=? and table_name=? and t.property_value = g.tablegroup_name")) {
                                ps.setObject(1, schema.getTableNameObj().getSchemaName());
                                ps.setObject(2, schema.getTableNameObj().getTableName());
                                try (ResultSet rs = ps.executeQuery()) {
                                    if (rs.next()) {
                                        shardCount = rs.getInt(1);
                                    } else {
                                        throw new SQLException(
                                                "table "
                                                        + schema.getTableNameObj().getFullName()
                                                        + " not exists");
                                    }
                                }
                            }
                            return shardCount;
                        }));
    }

    public static List<String> getSlotNames(HoloClient client, TableSchema schema)
            throws HoloClientException {
        return get(
                client.sql(
                        conn -> {
                            List<String> slotNames = new ArrayList<>();
                            try (PreparedStatement ps =
                                    conn.prepareStatement(
                                            "select slot_name from hologres.hg_replication_slot_properties, pg_publication_tables where schemaname=? and tablename=? and property_value=pubname;")) {
                                ps.setObject(1, schema.getTableNameObj().getSchemaName());
                                ps.setObject(2, schema.getTableNameObj().getTableName());
                                try (ResultSet rs = ps.executeQuery()) {
                                    while (rs.next()) {
                                        slotNames.add(rs.getString(1));
                                    }
                                }
                                if (slotNames.size() == 0) {
                                    throw new SQLException(
                                            "table "
                                                    + schema.getTableNameObj().getFullName()
                                                    + " not exists or not have any slot");
                                }
                            }
                            return slotNames;
                        }));
    }

    public static LinkedHashMap<String, TableName> getPartValueToPartitionTableMap(
            HoloClient client, TableSchema schema) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("with inh as ( \n");
        sb.append("    SELECT i.inhrelid, i.inhparent \n");
        sb.append("    FROM pg_catalog.pg_class c \n");
        sb.append("    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \n");
        sb.append("    LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent \n");
        sb.append("    where n.nspname=? and c.relname=? \n");
        sb.append(") \n");
        sb.append("select \n");
        sb.append("    n.nspname as schema_name, \n");
        sb.append("    c.relname as table_name, \n");
        sb.append("    inh.inhrelid, inh.inhparent, p.partstrat, \n");
        sb.append("    pg_get_expr(c.relpartbound, c.oid, true) as part_expr, \n");
        sb.append("    p.partdefid, \n");
        sb.append("    p.partnatts, \n");
        sb.append("    p.partattrs \n");
        sb.append("from inh \n");
        sb.append("join pg_catalog.pg_class c on inh.inhrelid = c.oid \n");
        sb.append("join pg_catalog.pg_namespace n on c.relnamespace = n.oid \n");
        sb.append(
                "join pg_partitioned_table p on p.partrelid = inh.inhparent order by schema_name, table_name \n");

        String sql = sb.toString();

        try {
            return get(
                    client.sql(
                            conn -> {
                                TableName childTable;
                                LinkedHashMap<String, TableName> childTableMap =
                                        new LinkedHashMap<>();

                                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                                    stmt.setString(1, schema.getTableNameObj().getSchemaName());
                                    stmt.setString(2, schema.getTableNameObj().getTableName());
                                    try (ResultSet rs = stmt.executeQuery()) {
                                        while (rs.next()) {

                                            String strategyStr = rs.getString("partstrat");
                                            if (!"l".equals(strategyStr)) {
                                                throw new SQLException(
                                                        "Only LIST partition is supported in holo.");
                                            }
                                            childTable =
                                                    TableName.quoteValueOf(
                                                            rs.getString("schema_name"),
                                                            rs.getString("table_name"));
                                            String partValue =
                                                    rs.getString("part_expr")
                                                            .replace("FOR VALUES IN (", "")
                                                            .replace(")", "")
                                                            .replaceAll("^'|'$", "");
                                            childTableMap.put(partValue, childTable);
                                        }
                                    }
                                    return childTableMap;
                                }
                            }));
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static HashMap<Integer, Long> getLatestBinlogLsn(
            HoloClient client, TableName tableName, Set<Integer> shardIds) {
        HashMap<Integer, Long> latestBinlogLsnMap = new HashMap<>(shardIds.size());
        try {
            get(
                    client.sql(
                            conn -> {
                                for (Integer shardId : shardIds) {
                                    String sql =
                                            String.format(
                                                    "SELECT * FROM hg_get_binlog_cursor('%s','LATEST',%s)",
                                                    tableName.getFullName(), shardId);
                                    try (Statement stmt = conn.createStatement()) {
                                        try (ResultSet rs = stmt.executeQuery(sql)) {
                                            if (rs.next()) {
                                                long lsn = rs.getLong("hg_binlog_lsn");
                                                latestBinlogLsnMap.put(shardId, lsn);
                                            }
                                        }
                                    }
                                }
                                return null;
                            }));
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
        return latestBinlogLsnMap;
    }

    public static HoloVersion getHoloVersion(HoloClient client) throws HoloClientException {
        return get(client.sql(ConnectionUtil::getHoloVersion));
    }

    public static String getOrCreateDefaultSlot(HoloClient client, TableName tableName)
            throws HoloClientException {
        return get(client.sql(conn -> ConnectionUtil.getOrCreateDefaultSlot(conn, tableName)));
    }

    private static <T> T get(CompletableFuture<T> future) throws HoloClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "interrupt", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HoloClientException) {
                throw (HoloClientException) cause;
            } else {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", cause);
            }
        }
    }
}
