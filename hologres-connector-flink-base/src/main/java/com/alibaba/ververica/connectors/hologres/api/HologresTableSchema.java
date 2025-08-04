package com.alibaba.ververica.connectors.hologres.api;

import com.alibaba.hologres.client.Command;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;

import java.io.Serializable;

/** A class wrapper of Hologres TableSchema class to distinguish with Flink's TableSchema class. */
public class HologresTableSchema implements Serializable {
    private final TableSchema hologresSchema;
    private final int shardCount;

    public HologresTableSchema(TableSchema hologresSchema, int shardCount) {
        this.hologresSchema = hologresSchema;
        this.shardCount = shardCount;
    }

    public static HologresTableSchema get(JDBCOptions jdbcOptions) {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
        holoConfig.setUsername(jdbcOptions.getUsername());
        holoConfig.setPassword(jdbcOptions.getPassword());
        holoConfig.setUseAKv4(jdbcOptions.isEnableAkv4());
        if (jdbcOptions.isEnableAkv4()) {
            holoConfig.setRegion(jdbcOptions.getAkv4Region());
        }
        try {
            HoloClient client = new HoloClient(holoConfig);
            TableSchema tableSchema = client.getTableSchema(jdbcOptions.getTable());
            int shardCount = Command.getShardCount(client, tableSchema);
            client.close();
            return new HologresTableSchema(tableSchema, shardCount);
        } catch (HoloClientException e) {
            throw new RuntimeException(e);
        }
    }

    public TableSchema get() {
        return this.hologresSchema;
    }

    public Column getColumn(String columnName) {
        Integer index = hologresSchema.getColumnIndex(columnName);
        if (index == null || index < 0) {
            throw new RuntimeException("Hologres table does not have column " + columnName);
        }
        return hologresSchema.getColumn(index);
    }

    public int getShardCount() {
        return shardCount;
    }
}
