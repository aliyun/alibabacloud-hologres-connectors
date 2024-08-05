package com.alibaba.ververica.connectors.hologres.source.bulkread;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.config.JDBCOptions;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/** HologresBulkReader. */
public class HologresBulkReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresBulkReader.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions jdbcOptions;
    private final String[] shardIds;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;

    private transient PgConnection conn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;

    public HologresBulkReader(
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] shardIds) {
        this.connectionParam = connectionParam;
        this.jdbcOptions = jdbcOptions;
        this.shardIds = shardIds;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public void open() throws IOException {
        String queryTemplate =
                JDBCUtils.getSimpleSelectFromStatement(jdbcOptions.getTable(), fieldNames);
        String query =
                String.format(
                        "%s WHERE hg_shard_id in (%s)",
                        queryTemplate, Arrays.toString(shardIds).replace("[", "").replace("]", ""));

        LOG.info("the bulk read query: {}", query);
        try {
            conn =
                    JDBCUtils.createConnection(
                                    jdbcOptions, jdbcOptions.getDbUrl(), /*sslModeConnection*/ true)
                            .unwrap(PgConnection.class);
            // The default statement_timeout in holo is 8 hours, but users may set this parameter
            // relatively small at the db level, which may affect full reading. Therefore, we
            // specifically set it to 24 hours. We do not recommend a full bulk reader
            // exceeding 24 hours. In this case, the concurrency should be increased or the user
            // should adjust the db level of statement_timeout.
            try (Statement stat = conn.createStatement()) {
                stat.execute("set statement_timeout = '24h';");
            } catch (SQLException e) {
                throw new IOException("set statement_timeout to 24h failed because", e);
            }
            // The default idle_in_transaction_session_timeout of holo is 10 minutes. If the user
            // processes data too slowly, it may time out.
            try (Statement stat = conn.createStatement()) {
                stat.execute(String.format("set idle_in_transaction_session_timeout = '1h';"));
            } catch (SQLException e) {
                throw new IOException("set statement_timeout to 1h failed because", e);
            }
            conn.setAutoCommit(false);
            statement =
                    conn.prepareStatement(
                            query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(connectionParam.getJdbcScanFetchSize());
            statement.setQueryTimeout(connectionParam.getScanTimeoutSeconds());
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new IOException(
                    String.format(
                            "Failed to execute query \"%s\" because %s",
                            query, ExceptionUtils.getStackTrace(e)));
        }
    }

    public RowData nextRecord() throws IOException {
        try {
            if (!resultSet.next()) {
                // reached End
                return null;
            }
            GenericRowData row = new GenericRowData(fieldNames.length);
            for (int i = 0; i < row.getArity(); i++) {
                row.setField(i, convert(fieldTypes[i], resultSet, i + 1));
            }
            return row;
        } catch (SQLException e) {
            throw new IOException("bulkRead get next record from resultSet failed because: " + e);
        }
    }

    private Object convert(DataType dataType, ResultSet resultSet, int resultSetIndex)
            throws SQLException {
        if (resultSet.getObject(resultSetIndex) == null) {
            return null;
        }
        switch (dataType.getLogicalType().getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return resultSet.getBytes(resultSetIndex);
            case CHAR:
            case VARCHAR:
                return StringData.fromString(resultSet.getString(resultSetIndex));
            case TINYINT:
                return resultSet.getByte(resultSetIndex);
            case SMALLINT:
                return resultSet.getShort(resultSetIndex);
            case INTEGER:
                return resultSet.getInt(resultSetIndex);
            case BIGINT:
                return resultSet.getLong(resultSetIndex);
            case FLOAT:
                return resultSet.getFloat(resultSetIndex);
            case DOUBLE:
                return resultSet.getDouble(resultSetIndex);
            case BOOLEAN:
                return resultSet.getBoolean(resultSetIndex);
            case DECIMAL:
                DecimalType logicalType = (DecimalType) dataType.getLogicalType();
                return DecimalData.fromBigDecimal(
                        resultSet.getBigDecimal(resultSetIndex),
                        logicalType.getPrecision(),
                        logicalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromTimestamp(resultSet.getTimestamp(resultSetIndex));
            case TIME_WITHOUT_TIME_ZONE:
                return (int)
                        (resultSet.getTime(resultSetIndex).toLocalTime().toNanoOfDay()
                                / 1_000_000L);
            case DATE:
                return (int) resultSet.getDate(resultSetIndex).toLocalDate().toEpochDay();
            case ARRAY:
                switch (dataType.getLogicalType().getChildren().get(0).getTypeRoot()) {
                    case BIGINT:
                        return new GenericArrayData(
                                (Long[]) resultSet.getArray(resultSetIndex).getArray());
                    case INTEGER:
                        return new GenericArrayData(
                                (Integer[]) resultSet.getArray(resultSetIndex).getArray());
                    case FLOAT:
                        return new GenericArrayData(
                                (Float[]) resultSet.getArray(resultSetIndex).getArray());
                    case DOUBLE:
                        return new GenericArrayData(
                                (Double[]) resultSet.getArray(resultSetIndex).getArray());
                    case BOOLEAN:
                        return new GenericArrayData(
                                (Boolean[]) resultSet.getArray(resultSetIndex).getArray());
                    case VARCHAR:
                        String[] values = (String[]) resultSet.getArray(resultSetIndex).getArray();
                        StringData[] arrStringObject = new StringData[values.length];
                        for (int i = 0; i < values.length; ++i) {
                            arrStringObject[i] = StringData.fromString(values[i]);
                        }
                        return new GenericArrayData(arrStringObject);
                    default:
                        throw new IllegalArgumentException("Unknown hologres type: " + dataType);
                }
            default:
                throw new IllegalArgumentException("Unknown hologres type: " + dataType);
        }
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
