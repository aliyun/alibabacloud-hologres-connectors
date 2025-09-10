package com.alibaba.hologres.connector.flink.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Objects;

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

    public static String[] getTargetFieldNames(
            org.apache.flink.table.api.TableSchema tableSchema, Integer[] targetColumnIndexes) {
        if (Objects.nonNull(targetColumnIndexes) && targetColumnIndexes.length > 0) {
            String[] fieldNames = new String[targetColumnIndexes.length];
            for (int idx = 0; idx < targetColumnIndexes.length; idx++) {
                fieldNames[idx] = tableSchema.getFieldNames()[targetColumnIndexes[idx]];
            }
            return fieldNames;
        }
        return tableSchema.getFieldNames();
    }
}
