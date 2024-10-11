package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Flink compatibility util. */
public class SchemaUtil {
    public static LogicalType[] getLogicalTypes(TableSchema tableSchema) {
        LogicalType[] logicalTypes = new LogicalType[tableSchema.getFieldCount()];
        final RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            logicalTypes[i] = rowType.getTypeAt(i);
        }
        return logicalTypes;
    }

    public static LogicalType[] getLogicalTypes(TableSchema tableSchema, String[] fieldNames) {
        LogicalType[] fieldTypes = new LogicalType[fieldNames.length];
        for (int idx = 0; idx < fieldNames.length; idx++) {
            fieldTypes[idx] = tableSchema.getFieldDataType(fieldNames[idx]).get().getLogicalType();
        }
        return fieldTypes;
    }
}
