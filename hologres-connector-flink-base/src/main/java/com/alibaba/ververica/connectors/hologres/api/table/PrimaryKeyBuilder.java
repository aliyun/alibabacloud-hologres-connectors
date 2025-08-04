package com.alibaba.ververica.connectors.hologres.api.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** An Util to transform RowData to a record of T, and only contains the data field to join. */
public class PrimaryKeyBuilder<T> implements Serializable {
    protected final LogicalType[] fieldTypes;
    protected final LogicalType[] indexColumnTypes;
    protected final RowData.FieldGetter[] pkFieldGetters;
    protected final String[] fieldNames;
    protected final List<String> pkFieldNames;
    protected final RowDataWriter<T> rowDataWriter;
    private final RowDataWriter.FieldWriter[] pkFieldWriter;

    public PrimaryKeyBuilder(
            String[] index,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            RowDataWriter<T> rowDataWriter,
            HologresTableSchema hologresTableSchema,
            HologresConnectionParam param) {
        this.rowDataWriter = rowDataWriter;
        this.pkFieldNames = new ArrayList<>();
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.pkFieldGetters = new RowData.FieldGetter[index.length];
        this.indexColumnTypes = new LogicalType[index.length];
        this.pkFieldWriter = new RowDataWriter.FieldWriter[index.length];
        for (int i = 0; i < index.length; i++) {
            int targetIdx = -1;
            for (int j = 0; j < fieldNames.length; j++) {
                if (fieldNames[j].equals(index[i])) {
                    targetIdx = j;
                    break;
                }
            }
            this.indexColumnTypes[i] = fieldTypes[targetIdx];
            this.pkFieldNames.add(fieldNames[targetIdx]);
            this.pkFieldGetters[i] = RowData.createFieldGetter(fieldTypes[targetIdx], i);
            Column holgoresColumn = hologresTableSchema.getColumn(fieldNames[targetIdx]);
            this.pkFieldWriter[i] =
                    RowDataWriter.createFieldWriter(
                            indexColumnTypes[i],
                            holgoresColumn.getType(),
                            holgoresColumn.getTypeName(),
                            rowDataWriter,
                            hologresTableSchema.get().getColumnIndex(fieldNames[targetIdx]),
                            param.isJdbcEnableRemoveU0000InText());
        }
    }

    public T buildPk(RowData rowData) {
        rowDataWriter.newRecord();
        for (int i = 0; i < pkFieldNames.size(); ++i) {
            pkFieldWriter[i].writeValue(pkFieldGetters[i].getFieldOrNull(rowData));
        }
        return rowDataWriter.complete();
    }
}
