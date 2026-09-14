package com.alibaba.hologres.connector.flink.source.bulkread;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import com.alibaba.hologres.client.copy.CopyFormat;
import com.alibaba.hologres.client.copy.out.CopyOutWrapper;
import com.alibaba.hologres.client.copy.out.arrow.accessor.BaseArrowArrayAccessor;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.utils.JDBCUtils;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Hologres bulk reader using COPY OUT with Arrow format.
 *
 * <p>Uses CopyOutWrapper to read data in Arrow columnar format, which provides higher throughput
 * than row-by-row SELECT queries.
 */
public class HologresCopyBulkReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HologresCopyBulkReader.class);

    private final HologresConnectionParam connectionParam;
    private final JDBCOptions jdbcOptions;
    private final String[] shardIds;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final String[] holoColumnTypes;
    private final String filterPredicate;
    private final long limit;

    private transient Connection conn;
    private transient CopyOutWrapper copyOutWrapper;
    private transient Iterator<RowData> currentBatchIterator;
    private transient TableSchema holoTableSchema;

    public HologresCopyBulkReader(
            HologresConnectionParam connectionParam,
            JDBCOptions jdbcOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] holoColumnTypes,
            String[] shardIds,
            String filterPredicate,
            long limit) {
        this.connectionParam = connectionParam;
        this.jdbcOptions = jdbcOptions;
        this.shardIds = shardIds;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.holoColumnTypes = holoColumnTypes;
        this.filterPredicate = filterPredicate;
        this.limit = limit;
    }

    /** Opens the COPY OUT connection and initializes the CopyOutWrapper. */
    public void open() throws IOException {
        LOG.info("Opening HologresCopyBulkReader for shards: {}", Arrays.toString(shardIds));
        try {
            conn =
                    JDBCUtils.createConnection(
                            jdbcOptions,
                            jdbcOptions.getDbUrl(),
                            true,
                            3,
                            "hologres-connector-flink-copy-bulkread");

            JDBCUtils.executeSql(
                    conn,
                    String.format(
                            "set statement_timeout = %s",
                            connectionParam.getStatementTimeoutSeconds()));
            JDBCUtils.executeSql(conn, "set idle_in_transaction_session_timeout = '1h';");
            if (connectionParam.isEnableServerlessComputing()) {
                JDBCUtils.executeSql(conn, "set hg_computing_resource = 'serverless';");
                JDBCUtils.executeSql(
                        conn,
                        String.format(
                                "set hg_experimental_serverless_computing_query_priority = '%d';",
                                connectionParam.getServerlessComputingQueryPriority()));
            }

            // Set target shards
            if (shardIds != null && shardIds.length > 0) {
                String shardList = String.join(",", shardIds);
                JDBCUtils.executeSql(
                        conn,
                        String.format("SET hg_experimental_target_shard_list = '%s'", shardList));
            }

            // Get table schema
            holoTableSchema =
                    com.alibaba.hologres.client.impl.util.ConnectionUtil.getTableSchema(
                            conn,
                            com.alibaba.hologres.client.model.TableName.valueOf(
                                    jdbcOptions.getTable()));

            // Create column name list
            List<String> readColumns = new ArrayList<>();
            Collections.addAll(readColumns, fieldNames);

            // Create CopyOutWrapper with Arrow format
            copyOutWrapper =
                    new CopyOutWrapper(
                            conn,
                            holoTableSchema,
                            readColumns,
                            getCopyFormat(connectionParam.isScanBulkReadCompressionEnabled()),
                            Collections.emptyList(),
                            buildCopyFilter(filterPredicate, limit),
                            connectionParam.getScanBulkReadMaxBufferSize());

            currentBatchIterator = Collections.emptyIterator();
            LOG.info("HologresCopyBulkReader opened successfully");
        } catch (SQLException e) {
            throw new IOException("Failed to open HologresCopyBulkReader", e);
        }
    }

    static CopyFormat getCopyFormat(boolean compressionEnabled) {
        return compressionEnabled ? CopyFormat.ARROW_LZ4 : CopyFormat.ARROW;
    }

    static String buildCopyFilter(String filterPredicate, long limit) {
        StringBuilder builder = new StringBuilder();
        if (filterPredicate != null && !filterPredicate.trim().isEmpty()) {
            builder.append("where ").append(filterPredicate.trim());
        }
        if (limit > 0) {
            if (builder.length() == 0) {
                builder.append("where true");
            }
            builder.append(" limit ").append(limit);
        }
        return builder.length() == 0 ? null : builder.toString();
    }

    static TimestampData timestampDataFromMicros(long micros) {
        long epochMillis = Math.floorDiv(micros, 1000);
        int nanoOfMillis = (int) Math.floorMod(micros, 1000) * 1000;
        return TimestampData.fromEpochMillis(epochMillis, nanoOfMillis);
    }

    /**
     * Returns the next RowData record, or null if no more records.
     *
     * @return the next RowData, or null if end of data.
     */
    public RowData nextRecord() throws IOException {
        while (true) {
            if (currentBatchIterator.hasNext()) {
                return currentBatchIterator.next();
            }
            // Try to get next batch
            if (!copyOutWrapper.hasNextBatch()) {
                return null;
            }
            VectorSchemaRoot root = copyOutWrapper.getVectorSchemaRoot();
            currentBatchIterator = convertArrowToRowData(root);
        }
    }

    private Iterator<RowData> convertArrowToRowData(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        if (rowCount == 0) {
            return Collections.emptyIterator();
        }

        int fieldCount = fieldNames.length;
        List<RowData> rows = new ArrayList<>(rowCount);

        for (int rowId = 0; rowId < rowCount; rowId++) {
            GenericRowData row = new GenericRowData(fieldCount);
            for (int col = 0; col < fieldCount; col++) {
                FieldVector vector = root.getFieldVectors().get(col);
                if (vector.isNull(rowId)) {
                    row.setField(col, null);
                } else {
                    row.setField(
                            col,
                            readFromArrowVector(
                                    vector,
                                    rowId,
                                    fieldTypes[col],
                                    holoColumnTypes[col],
                                    holoTableSchema.getColumn(
                                            holoTableSchema.getColumnIndex(fieldNames[col]))));
                }
            }
            rows.add(row);
        }
        return rows.iterator();
    }

    private Object readFromArrowVector(
            FieldVector vector, int rowId, DataType dataType, String holoTypeName, Column column) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case BOOLEAN:
                if (vector instanceof BitVector) {
                    return ((BitVector) vector).get(rowId) != 0;
                } else if (vector instanceof UInt1Vector) {
                    return ((UInt1Vector) vector).get(rowId) != 0;
                }
                break;
            case TINYINT:
                if (vector instanceof SmallIntVector) {
                    return (byte) ((SmallIntVector) vector).get(rowId);
                }
                break;
            case SMALLINT:
                if (vector instanceof SmallIntVector) {
                    return ((SmallIntVector) vector).get(rowId);
                }
                break;
            case INTEGER:
                if (vector instanceof IntVector) {
                    return ((IntVector) vector).get(rowId);
                }
                break;
            case BIGINT:
                if (vector instanceof BigIntVector) {
                    return ((BigIntVector) vector).get(rowId);
                }
                break;
            case FLOAT:
                if (vector instanceof Float4Vector) {
                    return ((Float4Vector) vector).get(rowId);
                }
                break;
            case DOUBLE:
                if (vector instanceof Float8Vector) {
                    return ((Float8Vector) vector).get(rowId);
                }
                break;
            case CHAR:
            case VARCHAR:
                if (vector instanceof VarCharVector) {
                    byte[] bytes = ((VarCharVector) vector).get(rowId);
                    return StringData.fromBytes(bytes);
                }
                break;
            case BINARY:
            case VARBINARY:
                if (vector instanceof VarBinaryVector) {
                    return ((VarBinaryVector) vector).get(rowId);
                }
                break;
            case DECIMAL:
                if (vector instanceof DecimalVector) {
                    DecimalType decimalType = (DecimalType) dataType.getLogicalType();
                    BigDecimal bd = ((DecimalVector) vector).getObject(rowId);
                    return DecimalData.fromBigDecimal(
                            bd, decimalType.getPrecision(), decimalType.getScale());
                }
                break;
            case DATE:
                if (vector instanceof DateDayVector) {
                    return ((DateDayVector) vector).get(rowId);
                }
                break;
            case TIME_WITHOUT_TIME_ZONE:
                if (vector instanceof TimeMicroVector) {
                    // Arrow stores time in microseconds, Flink needs milliseconds
                    return (int) (((TimeMicroVector) vector).get(rowId) / 1000L);
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if ("timestamptz".equals(holoTypeName)) {
                    if (vector instanceof DateMilliVector) {
                        long millis = ((DateMilliVector) vector).get(rowId);
                        return TimestampData.fromTimestamp(new Timestamp(millis));
                    }
                } else {
                    if (vector instanceof TimeStampMicroVector) {
                        long micros = ((TimeStampMicroVector) vector).get(rowId);
                        return timestampDataFromMicros(micros);
                    }
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if ("timestamptz".equals(holoTypeName)) {
                    if (vector instanceof DateMilliVector) {
                        long millis = ((DateMilliVector) vector).get(rowId);
                        return TimestampData.fromInstant(Instant.ofEpochMilli(millis));
                    }
                }
                break;
            case ARRAY:
                if (vector instanceof ListVector) {
                    return readArrayFromListVector((ListVector) vector, rowId, dataType, column);
                }
                break;
            default:
                break;
        }
        throw new IllegalArgumentException(
                "Unsupported type conversion from Arrow vector "
                        + vector.getClass().getSimpleName()
                        + " to Flink type "
                        + dataType);
    }

    /**
     * Reads an array value from a ListVector using holo-client's BaseArrowArrayAccessor.
     *
     * <p>BaseArrowArrayAccessor.get(rowId) returns typed Java arrays (Boolean[], Integer[], Long[],
     * Float[], Double[], String[], etc.), which are then converted to Flink's GenericArrayData.
     */
    private Object readArrayFromListVector(
            ListVector listVector, int rowId, DataType dataType, Column column) {
        BaseArrowArrayAccessor accessor =
                new BaseArrowArrayAccessor(listVector, column.getArrayElementType());
        Object result = accessor.get(rowId);
        if (result == null) {
            return null;
        }
        // BaseArrowArrayAccessor.get() returns typed Java arrays.
        // GenericArrayData accepts Object[] or primitive arrays.
        if (result instanceof String[]) {
            String[] strArr = (String[]) result;
            StringData[] stringDataArr = new StringData[strArr.length];
            for (int i = 0; i < strArr.length; i++) {
                stringDataArr[i] = strArr[i] != null ? StringData.fromString(strArr[i]) : null;
            }
            return new GenericArrayData(stringDataArr);
        } else if (result instanceof Boolean[]) {
            return new GenericArrayData((Boolean[]) result);
        } else if (result instanceof Integer[]) {
            return new GenericArrayData((Integer[]) result);
        } else if (result instanceof Long[]) {
            return new GenericArrayData((Long[]) result);
        } else if (result instanceof Float[]) {
            return new GenericArrayData((Float[]) result);
        } else if (result instanceof Double[]) {
            return new GenericArrayData((Double[]) result);
        } else if (result instanceof Short[]) {
            return new GenericArrayData((Short[]) result);
        } else if (result instanceof List) {
            return new GenericArrayData(((List<?>) result).toArray());
        }
        throw new IllegalArgumentException(
                "Unsupported array result type: " + result.getClass().getName());
    }

    /** Closes the COPY OUT connection and releases resources. */
    public void close() throws IOException {
        if (copyOutWrapper != null) {
            try {
                copyOutWrapper.close();
            } catch (IOException e) {
                LOG.warn("Failed to close CopyOutWrapper", e);
            }
            copyOutWrapper = null;
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new IOException("Failed to close connection", e);
            }
            conn = null;
        }
    }
}
