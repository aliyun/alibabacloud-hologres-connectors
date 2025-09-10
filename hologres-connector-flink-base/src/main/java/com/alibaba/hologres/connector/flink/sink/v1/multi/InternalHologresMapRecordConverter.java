package com.alibaba.hologres.connector.flink.sink.v1.multi;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.connector.flink.api.HologresRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The source data is of type {@link Record}, requiring no conversion—only interface compatibility
 * is needed.
 */
public class InternalHologresMapRecordConverter
        implements HologresRecordConverter<Map<String, Object>, Record> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(InternalHologresMapRecordConverter.class);

    private final TableSchema tableSchema;
    private final boolean dataTypeTolerant;
    private final boolean extraColumnTolerant;
    private final Set<String> ignoredColumns;

    public InternalHologresMapRecordConverter(
            TableSchema tableSchema, boolean dataTypeTolerant, boolean extraColumnTolerant) {
        this.tableSchema = tableSchema;
        this.dataTypeTolerant = dataTypeTolerant;
        this.extraColumnTolerant = extraColumnTolerant;
        this.ignoredColumns = new HashSet<>();
    }

    @Override
    public Record convertFrom(Map<String, Object> record) {
        if (dataTypeTolerant) {
            return dataTypeTolerantConvertFrom(record);
        }
        Put put = new Put(tableSchema);
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            if (extraColumnTolerant && tableSchema.getColumnIndex(entry.getKey()) == null) {
                if (!ignoredColumns.contains(entry.getKey())) {
                    ignoredColumns.add(entry.getKey());
                    LOG.warn(
                            "can not found column named {} in {}, because extraColumnTolerant is true, ignore this column.",
                            entry.getKey(),
                            tableSchema);
                }
                continue;
            }
            put.setObject(entry.getKey(), entry.getValue());
        }
        return put.getRecord();
    }

    @Override
    public Map<String, Object> convertTo(Record record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record convertToPrimaryKey(Map<String, Object> record) {
        throw new UnsupportedOperationException();
    }

    private Record dataTypeTolerantConvertFrom(Map<String, Object> record) {
        Put put = new Put(tableSchema);
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            if (tableSchema.getColumnIndex(entry.getKey()) == null) {
                throw new InvalidParameterException(
                        "can not found column named " + entry.getKey() + " in " + tableSchema);
            }
            Column column = tableSchema.getColumn(tableSchema.getColumnIndex(entry.getKey()));
            Object value = entry.getValue();
            if (value == null) {
                put.setObject(entry.getKey(), null);
                continue;
            }
            switch (column.getType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                    if (!(value instanceof String)) {
                        value = String.valueOf(value);
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    if (!(value instanceof Boolean)) {
                        value = Boolean.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    if (!(value instanceof byte[])) {
                        value = String.valueOf(value).getBytes();
                    }
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    if (!(value instanceof BigDecimal)) {
                        value = new java.math.BigDecimal(String.valueOf(value));
                    }
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                    if (!(value instanceof Short)) {
                        value = Short.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.INTEGER:
                    if (!(value instanceof Integer)) {
                        value = Integer.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.BIGINT:
                    if (!(value instanceof Long)) {
                        value = Long.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.DATE:
                    if (!(value instanceof java.sql.Date)) {
                        value = java.sql.Date.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.TIME:
                    if (!(value instanceof java.sql.Time)) {
                        value = java.sql.Time.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    if (!(value instanceof Float)) {
                        value = Float.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.DOUBLE:
                    if (!(value instanceof Double)) {
                        value = Double.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    if (!(value instanceof java.sql.Timestamp)) {
                        value = java.sql.Timestamp.valueOf(String.valueOf(value));
                    }
                    break;
                case Types.ARRAY:
                    break;
                case Types.OTHER:
                    if (column.getTypeName().equals("json")
                            || column.getTypeName().equals("jsonb")) {
                        if (!(value instanceof String)) {
                            value = String.valueOf(value);
                        }
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Hologres sink does not support data type %s for now",
                                    column.getType()));
            }
            put.setObject(entry.getKey(), value);
        }
        return put.getRecord();
    }
}
