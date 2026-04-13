package com.alibaba.hologres.client.copy.out;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.out.arrow.ArrowVectorAccessorUtil;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class CopyOutWrapper implements AutoCloseable {
    CopyOutContext context;

    public CopyOutWrapper(
            Connection conn,
            String tableName,
            List<String> columns,
            CopyFormat copyFormat,
            List<Integer> shards,
            String filter,
            int maxCellBufferSize)
            throws SQLException {
        this(
                conn,
                ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName)),
                columns,
                copyFormat,
                shards,
                filter,
                maxCellBufferSize);
    }

    public CopyOutWrapper(
            Connection conn,
            TableSchema schema,
            List<String> columns,
            CopyFormat copyFormat,
            List<Integer> shards,
            String filter,
            int maxCellBufferSize) {
        this.context =
                new CopyOutContext(
                        conn, schema, columns, copyFormat, shards, filter, maxCellBufferSize);
    }

    public boolean hasNextBatch() throws IOException {
        context.checkContextOpen();
        return context.reader.nextBatch();
    }

    public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
        context.checkContextOpen();
        return context.reader.getCurrentValue();
    }

    public List<Record> getRecords() throws IOException {
        context.checkContextOpen();
        return ArrowVectorAccessorUtil.convertVectorSchemaRootToRecords(
                context.reader.getCurrentValue(), context.getSchema(), context.getColumns());
    }

    @Override
    public void close() throws IOException {
        context.close();
    }
}
