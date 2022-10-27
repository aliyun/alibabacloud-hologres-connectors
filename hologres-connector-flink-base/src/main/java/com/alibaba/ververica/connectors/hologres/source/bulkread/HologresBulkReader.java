package com.alibaba.ververica.connectors.hologres.source.bulkread;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.utils.HologresUtils;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/** HologresBulkReader. */
public class HologresBulkReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresBulkReader.class);

    private final JDBCOptions jdbcOptions;
    private final String regexSplitter;
    private final int shardId;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;

    private transient Connection conn;
    private transient BufferedReader reader;
    private transient String line;
    private transient InputStreamReader streamReader;
    private transient PGCopyInputStream in;

    public HologresBulkReader(
            JDBCOptions jdbcOptions, String[] fieldNames, DataType[] fieldTypes, int shardId) {
        this.jdbcOptions = jdbcOptions;
        this.regexSplitter = "\\" + jdbcOptions.getDelimiter();
        this.shardId = shardId;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public void open() throws IOException {
        StringBuilder builder = new StringBuilder();
        String queryTemplate =
                JDBCUtils.getSimpleSelectFromStatement(jdbcOptions.getTable(), fieldNames);
        builder.append("COPY (");
        builder.append(queryTemplate);
        builder.append(String.format(" WHERE hg_shard_id in (%s)", shardId));
        if (StringUtils.isNotBlank(jdbcOptions.getFilter())) {
            builder.append(" AND ");
            builder.append(jdbcOptions.getFilter());
        }

        builder.append(") TO STDOUT WITH DELIMITER e'%s';");
        String query = String.format(builder.toString(), jdbcOptions.getDelimiter());

        LOG.info("the bulk read query: {}", query);
        try {
            conn = JDBCUtils.createConnection(jdbcOptions);
            in = new PGCopyInputStream((PGConnection) conn, query);
        } catch (SQLException e) {
            throw new IOException(
                    String.format(
                            "Failed creating PGCopyInputStream for ShardId %s because %s",
                            shardId, ExceptionUtils.getStackTrace(e)));
        }

        streamReader = new InputStreamReader(in);
        reader = new BufferedReader(streamReader);
    }

    public RowData nextRecord() throws IOException {
        if ((line = reader.readLine()) == null) {
            // reached End
            return null;
        }

        GenericRowData row = new GenericRowData(fieldNames.length);
        String[] values = line.split(regexSplitter, -1);

        assert (row.getArity() == values.length);

        for (int i = 0; i < row.getArity(); i++) {
            row.setField(i, convert(fieldTypes[i], values[i]));
        }
        return row;
    }

    private Object convert(DataType t, String v) {
        if (v.equals("\\N")) {
            return null;
        }

        return HologresUtils.convertStringToInternalObject(v, t);
    }

    public void close() throws IOException {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
