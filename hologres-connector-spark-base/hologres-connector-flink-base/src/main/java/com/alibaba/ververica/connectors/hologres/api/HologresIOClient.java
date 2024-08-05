package com.alibaba.ververica.connectors.hologres.api;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.io.IOException;
import java.io.Serializable;

/** Abstract for different Holgores IO interface. */
public abstract class HologresIOClient<T> implements Serializable {
    protected final HologresConnectionParam param;
    protected final String[] fieldNames;
    protected final LogicalType[] logicalTypes;

    public HologresIOClient(HologresConnectionParam param, TableSchema tableSchema) {
        this.param = param;
        this.fieldNames = tableSchema.getFieldNames();
        this.logicalTypes = getLogicalTypes(tableSchema);
    }

    public abstract void open(RuntimeContext runtimeContext) throws IOException;

    public abstract void close() throws IOException, HoloClientException;

    public static LogicalType[] getLogicalTypes(TableSchema tableSchema) {
        LogicalType[] logicalTypes = new LogicalType[tableSchema.getFieldCount()];
        final RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            logicalTypes[i] = rowType.getTypeAt(i);
        }
        return logicalTypes;
    }
}
