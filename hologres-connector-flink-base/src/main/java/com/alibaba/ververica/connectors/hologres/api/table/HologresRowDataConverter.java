package com.alibaba.ververica.connectors.hologres.api.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/** HologresTableRecordConverter is used for Table API. */
public class HologresRowDataConverter<T> implements HologresRecordConverter<RowData, T> {
    private final RowData.FieldGetter[] fieldGetters;
    private final RowDataWriter.FieldWriter[] fieldWriters;
    private final RowDataReader.FieldReader<T>[] fieldReaders;
    private final String[] fieldNames;
    private final Map<String, Integer> columnNameToIndex;
    private final int fieldLength;
    private final RowDataWriter<T> rowDataWriter;
    private final RowDataReader<T> rowDataReader;
    private PrimaryKeyBuilder<T> primaryKeyBuilder;

    public HologresRowDataConverter(
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresConnectionParam param,
            RowDataWriter<T> rowDataWriter,
            RowDataReader<T> rowDataReader,
            HologresTableSchema hologresTableSchema) {
        this(
                new String[] {},
                fieldNames,
                fieldTypes,
                param,
                rowDataWriter,
                rowDataReader,
                hologresTableSchema);
    }

    public HologresRowDataConverter(
            String[] primaryKeyIndex,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresConnectionParam param,
            RowDataWriter<T> rowDataWriter,
            RowDataReader<T> rowDataReader,
            HologresTableSchema hologresTableSchema) {
        this.fieldNames = fieldNames;
        this.columnNameToIndex = new HashMap<>();
        this.rowDataWriter = rowDataWriter;
        this.rowDataReader = rowDataReader;
        this.fieldLength = fieldNames.length;
        this.fieldGetters = new RowData.FieldGetter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            this.fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        this.fieldWriters = new RowDataWriter.FieldWriter[fieldLength];
        this.fieldReaders = new RowDataReader.FieldReader[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            columnNameToIndex.put(fieldNames[i], i);
            Column holgoresColumn = hologresTableSchema.getColumn(fieldNames[i]);
            rowDataWriter.checkHologresTypeSupported(
                    holgoresColumn.getType(), holgoresColumn.getTypeName());
            rowDataReader.checkHologresTypeSupported(
                    holgoresColumn.getType(), holgoresColumn.getTypeName());
            this.fieldWriters[i] =
                    RowDataWriter.createFieldWriter(
                            fieldTypes[i],
                            holgoresColumn.getType(),
                            holgoresColumn.getTypeName(),
                            rowDataWriter,
                            hologresTableSchema.get().getColumnIndex(fieldNames[i]),
                            param.getJdbcOptions().getDelimiter());
            this.fieldReaders[i] =
                    RowDataReader.createFieldReader(
                            fieldTypes[i],
                            holgoresColumn.getType(),
                            holgoresColumn.getTypeName(),
                            rowDataReader,
                            i);
        }
        if (primaryKeyIndex.length > 0) {
            this.primaryKeyBuilder =
                    new PrimaryKeyBuilder<T>(
                            primaryKeyIndex,
                            fieldNames,
                            fieldTypes,
                            rowDataWriter.copy(),
                            hologresTableSchema,
                            param);
        }
        validateDataTypeMapping(fieldTypes, hologresTableSchema);
    }

    @Override
    public T convertFrom(RowData record) {
        rowDataWriter.newRecord();
        for (int i = 0; i < fieldLength; ++i) {
            fieldWriters[i].writeValue(fieldGetters[i].getFieldOrNull(record));
        }
        return rowDataWriter.complete();
    }

    @Override
    public RowData convertTo(T record) {
        if (record == null) {
            return null;
        }
        GenericRowData resultRow = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldLength; i++) {
            resultRow.setField(i, fieldReaders[i].readValue(record));
        }
        return resultRow;
    }

    @Override
    public T convertToPrimaryKey(RowData record) {
        return this.primaryKeyBuilder.buildPk(record);
    }

    private void validateDataTypeMapping(
            LogicalType[] fieldTypes, HologresTableSchema hologresTableSchema) {
        for (int i = 0; i < fieldNames.length; i++) {
            boolean matched;
            Column hologresColumn = hologresTableSchema.getColumn(fieldNames[i]);
            LogicalType flinkType = fieldTypes[i];
            switch (hologresColumn.getType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR);
                    break;
                case Types.OTHER:
                    if ("roaringbitmap".equals(hologresColumn.getTypeName())) {
                        matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.VARBINARY);
                    } else {
                        matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR);
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.BOOLEAN);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    matched =
                            flinkType.getTypeRoot().equals(LogicalTypeRoot.VARBINARY)
                                    || flinkType.getTypeRoot().equals(LogicalTypeRoot.BINARY);
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.DECIMAL);
                    break;
                case Types.SMALLINT:
                    // holo not support TINYINT, SMALLINT should compatible with flink TINYINT
                    matched =
                            flinkType.getTypeRoot().equals(LogicalTypeRoot.SMALLINT)
                                    || flinkType.getTypeRoot().equals(LogicalTypeRoot.TINYINT);
                    break;
                case Types.INTEGER:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.INTEGER);
                    break;
                case Types.DATE:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.DATE);
                    break;
                case Types.BIGINT:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.BIGINT);
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.FLOAT);
                    break;
                case Types.DOUBLE:
                    matched = flinkType.getTypeRoot().equals(LogicalTypeRoot.DOUBLE);
                    break;
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    matched =
                            flinkType
                                            .getTypeRoot()
                                            .equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                                    || flinkType
                                            .getTypeRoot()
                                            .equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE)
                                    || flinkType
                                            .getTypeRoot()
                                            .equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                    break;
                case Types.ARRAY:
                    matched =
                            flinkType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR)
                                    || (flinkType.getTypeRoot().equals(LogicalTypeRoot.ARRAY)
                                            && (flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.BOOLEAN)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.VARCHAR)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.SMALLINT)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.INTEGER)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.FLOAT)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.DOUBLE)
                                                    || flinkType
                                                            .getChildren()
                                                            .get(0)
                                                            .getTypeRoot()
                                                            .equals(LogicalTypeRoot.BIGINT)));
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Hologres sink does not support column %s with data type %s for now",
                                    fieldNames[i], hologresColumn.getTypeName()));
            }
            if (!matched) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column: %s type does not match: flink row type: %s, hologres type: %s",
                                fieldNames[i], fieldTypes[i], hologresColumn.getTypeName()));
            }
        }
    }
}
