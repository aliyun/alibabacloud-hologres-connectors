/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import org.postgresql.model.TableSchema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 常用查询的封装.
 */
public class Command {

	public static int getShardCount(HoloClient client, TableSchema schema) throws HoloClientException {
		return get(client.sql(conn -> {
			int shardCount = -1;
			try (PreparedStatement ps = conn.prepareStatement("select g.property_value from hologres.hg_table_properties t,hologres.hg_table_group_properties g\n" +
					"where t.property_key='table_group' and g.property_key='shard_count' and table_namespace=? and table_name=? and t.property_value = g.tablegroup_name")) {
				ps.setObject(1, schema.getTableNameObj().getSchemaName());
				ps.setObject(2, schema.getTableNameObj().getTableName());
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						shardCount = rs.getInt(1);
					} else {
						throw new SQLException("table " + schema.getTableNameObj().getFullName() + " not exists");
					}
				}
			}
			return shardCount;
		}));
	}

	public static String getSlotName(HoloClient client, TableSchema schema) throws HoloClientException {
		return get(client.sql(conn -> {
			String slotName = "";
			try (PreparedStatement ps = conn.prepareStatement("select slot_name from hologres.hg_replication_slot_properties, pg_publication_tables where tablename=? and property_value=pubname;")) {
				ps.setObject(1, schema.getTableNameObj().getTableName());
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						slotName = rs.getString(1);
					} else {
						throw new SQLException("table " + schema.getTableNameObj().getFullName() + " not exists or have not any slot");
					}
				}
			}
			return slotName;
		}));
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
