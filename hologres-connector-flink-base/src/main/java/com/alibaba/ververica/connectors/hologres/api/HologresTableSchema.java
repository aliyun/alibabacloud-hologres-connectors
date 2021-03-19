package com.alibaba.ververica.connectors.hologres.api;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import org.postgresql.model.Column;
import org.postgresql.model.TableSchema;

import java.io.Serializable;

/** A class wrapper of Hologres TableSchema class to distinguish with Flink's TableSchema class. */
public class HologresTableSchema implements Serializable {
    private final TableSchema hologresSchema;

    public HologresTableSchema(TableSchema hologresSchema) {
        this.hologresSchema = hologresSchema;
    }

    public static HologresTableSchema get(JDBCOptions jdbcOptions) {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(jdbcOptions.getDbUrl());
        holoConfig.setUsername(jdbcOptions.getUsername());
        holoConfig.setPassword(jdbcOptions.getPassword());
        try {
            HoloClient client = new HoloClient(holoConfig);
            TableSchema tableSchema = client.getTableSchema(jdbcOptions.getTable());
            client.close();
            return new HologresTableSchema(tableSchema);
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
}
