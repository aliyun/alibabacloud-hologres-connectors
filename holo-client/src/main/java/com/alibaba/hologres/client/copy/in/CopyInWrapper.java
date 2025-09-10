package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CopyInWrapper implements AutoCloseable {
    private final CopyInContext context;

    /** copy in 指定的部分字段. */
    public CopyInWrapper(
            Connection conn,
            String tableName,
            List<String> columns,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize) {
        this.context =
                new CopyInContext(
                        conn,
                        tableName,
                        columns,
                        copyFormat,
                        copyMode,
                        onConflictAction,
                        maxCellBufferSize);
    }

    /* copy in 表的所有字段. */
    public CopyInWrapper(
            Connection conn,
            TableSchema schema,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize) {
        this(
                conn,
                schema.getTableNameObj().getFullName(),
                Arrays.stream(schema.getColumnSchema())
                        .map(Column::getName)
                        .collect(Collectors.toList()),
                copyFormat,
                copyMode,
                onConflictAction,
                maxCellBufferSize);
    }

    /* 传入一个Record, 选择其set的所有字段, 要求后续写入的Record也set相同的字段. */
    public CopyInWrapper(
            Connection conn,
            Record record,
            CopyFormat copyFormat,
            CopyMode copyMode,
            OnConflictAction onConflictAction,
            int maxCellBufferSize) {
        TableSchema schema = record.getSchema();
        List<String> columns = new ArrayList<>();
        for (int i = 0; i < schema.getColumnSchema().length; ++i) {
            if (record.isSet(i)) {
                columns.add(schema.getColumn(i).getName());
            }
        }
        this.context =
                new CopyInContext(
                        conn,
                        record.getSchema().getTableNameObj().getFullName(),
                        columns,
                        copyFormat,
                        copyMode,
                        onConflictAction,
                        maxCellBufferSize);
    }

    public TableSchema getSchema() throws IOException {
        return context.getSchema();
    }

    public void putRecord(Record record) throws IOException {
        // insert or replace 模式下，需要将 record 没有设置的字段设置为 null或者默认值
        if (context.getOnConflictAction() == OnConflictAction.INSERT_OR_REPLACE) {
            CopyUtil.prepareRecordForCopy(record, context.getColumns(), true, null);
        }
        if (context.IsNotInitialized()) {
            context.init();
        }
        checkColumnSet(record);
        context.checkContextOpen();
        context.recordOs.putRecord(record);
    }

    private void checkColumnSet(Record record) throws IOException {
        // table id 必须一致
        if (!record.getSchema().getTableId().equals(context.getSchema().getTableId())) {
            throw new IOException("record schema is not equal to context schema");
        }
        // 仅保证列数一致, 不去一一比较字段名, 避免性能问题
        if (record.getSchema().getColumnSchema().length
                != context.getSchema().getColumnSchema().length) {
            throw new IOException("record column number is not equal to context column number");
        }
        // 要求传入的record的bitSet与context的needSetColumns一致
        if (!record.getBitSet().equals(context.getNeedSetColumns())) {
            for (int i = 0; i < record.getBitSet().length(); ++i) {
                if (record.getBitSet().get(i) != context.getNeedSetColumns().get(i)) {
                    String columnName = record.getSchema().getColumn(i).getName();
                    if (record.getBitSet().get(i)) {
                        throw new IOException(
                                String.format(
                                        "Column %s should not be set, only the following fields(Specified when initializing CopyInWrapper) can be set: %s",
                                        columnName, context.getColumns()));
                    } else {
                        throw new IOException(
                                String.format(
                                        "Column %s should be set, because it is included the following fields(Specified when initializing CopyInWrapper): %s",
                                        columnName, context.getColumns()));
                    }
                }
            }
        }
    }

    public void flush() throws IOException {
        context.checkContextOpen();
        context.inOs.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        context.close();
    }
}
