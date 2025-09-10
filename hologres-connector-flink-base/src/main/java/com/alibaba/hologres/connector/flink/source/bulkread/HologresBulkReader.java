package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

/** HologresBulkReader. */
public class HologresBulkReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresBulkReader.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions jdbcOptions;
    private final String[] shardIds;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final String[] holoColumnTypes;
    private final boolean forSnapshotRead;

    private transient PgConnection conn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;

    private final String filterPredicate;
    private final long limit;

    public HologresBulkReader(
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] holoColumnTypes,
            String[] shardIds,
            boolean forSnapshotRead,
            String filterPredicate,
            long limit) {
        this.connectionParam = connectionParam;
        this.jdbcOptions = jdbcOptions;
        this.shardIds = shardIds;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.holoColumnTypes = holoColumnTypes;
        this.forSnapshotRead = forSnapshotRead;
        this.filterPredicate = filterPredicate;
        this.limit = limit;
    }

    public void open() throws IOException {
        String queryTemplate =
                JDBCUtils.getSimpleSelectFromStatement(jdbcOptions.getTable(), fieldNames);
        String query =
                String.format(
                        "%s WHERE hg_shard_id in (%s)",
                        queryTemplate, Arrays.toString(shardIds).replace("[", "").replace("]", ""));

        if (Objects.nonNull(filterPredicate) && !"".equals(filterPredicate)) {
            query = query + " and " + filterPredicate;
        }

        if (limit > 0) {
            query = query + " limit " + limit;
        }

        LOG.info("the bulk read query: {}", query);
        try {
            conn =
                    JDBCUtils.createConnection(
                                    jdbcOptions,
                                    jdbcOptions.getDbUrl(), /*sslModeConnection*/
                                    true, /*maxRetryCount*/
                                    3, /*appName*/
                                    "hologres-connector-flink-bulkread")
                            .unwrap(PgConnection.class);
            // The default statement_timeout in holo is 8 hours, but users may set this parameter
            // relatively small at the db level, which may affect full reading. Therefore, we
            // specifically set it.
            JDBCUtils.executeSql(
                    conn,
                    String.format(
                            "set statement_timeout = %s",
                            connectionParam.getStatementTimeoutSeconds()));
            // The default idle_in_transaction_session_timeout of holo is 10 minutes. If the user
            // processes data too slowly, it may time out.
            JDBCUtils.executeSql(conn, "set idle_in_transaction_session_timeout = '1h';");
            if (connectionParam.isEnableServerlessComputing()) {
                JDBCUtils.executeSql(conn, "set hg_computing_resource = 'serverless';");
                JDBCUtils.executeSql(
                        conn,
                        String.format(
                                "set hg_experimental_serverless_computing_query_priority = '%d';",
                                connectionParam.getServerlessComputingQueryPriority()));
            }
            conn.setAutoCommit(false);
            statement =
                    conn.prepareStatement(
                            query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(connectionParam.getScanFetchSize());
            statement.setQueryTimeout(connectionParam.getScanTimeoutSeconds());
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            throw new IOException(
                    String.format(
                            "Failed to execute query \"%s\" because %s",
                            query, ExceptionUtils.getStackTrace(e)));
        }
    }

    public Tuple3<RowData, Integer, Long> nextRecord() throws IOException {
        try {
            if (!resultSet.next()) {
                // reached End
                return null;
            }

            GenericRowData row = new GenericRowData(fieldNames.length);

            int shardId = -1;
            long sequence = -1;
            // resultSet index starts at 1
            int offset = 1;

            // The first two fields are shardId and sequence
            if (forSnapshotRead) {
                shardId = resultSet.getInt(1);
                sequence = resultSet.getLong(2);
                offset = 3;
            }
            for (int i = 0; i < fieldNames.length; i++) {
                row.setField(i, convert(holoColumnTypes[i], fieldTypes[i], resultSet, i + offset));
            }
            return new Tuple3<>(row, shardId, sequence);
        } catch (SQLException e) {
            throw new IOException("bulkRead get next record from resultSet failed because: " + e);
        }
    }

    private Object convert(
            String holoTypeName, DataType dataType, ResultSet resultSet, int resultSetIndex)
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
                if (holoTypeName.equals("timestamptz")) {
                    // hologres timestamptz -> flink timestamp
                    return TimestampData.fromTimestamp(resultSet.getTimestamp(resultSetIndex));
                } else {
                    // hologres timestamp -> flink timestamp
                    return TimestampData.fromTimestamp(resultSet.getTimestamp(resultSetIndex));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (holoTypeName.equals("timestamptz")) {
                    // hologres timestamptz -> flink timestamp_ltz
                    return TimestampData.fromInstant(
                            resultSet.getTimestamp(resultSetIndex).toInstant());
                } else {
                    // hologres timestamp -> flink timestamp_ltz
                    throw new UnsupportedOperationException(
                            "convert hologres timestamp to flink timestamp_ltz is unsupported now");
                }

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
