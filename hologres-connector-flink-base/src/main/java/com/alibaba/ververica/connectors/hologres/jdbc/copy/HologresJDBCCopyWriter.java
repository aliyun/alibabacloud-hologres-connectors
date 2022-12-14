package com.alibaba.ververica.connectors.hologres.jdbc.copy;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.copy.CopyInOutputStream;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.RecordOutputStream;
import com.alibaba.hologres.client.copy.RecordTextOutputStream;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.utils.RecordChecker;
import com.alibaba.hologres.org.postgresql.PGProperty;
import com.alibaba.hologres.org.postgresql.copy.CopyIn;
import com.alibaba.hologres.org.postgresql.copy.CopyManager;
import com.alibaba.hologres.org.postgresql.core.BaseConnection;
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import com.alibaba.ververica.connectors.hologres.api.HologresRecordConverter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.api.table.HologresRowDataConverter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCRecordReader;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCRecordWriter;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_IGNORE;
import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_UPDATE;

/** An IO writer implementation for JDBC. */
public class HologresJDBCCopyWriter<T> extends HologresWriter<T> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresJDBCCopyWriter.class);
    private final int frontendOffset;
    private final int numFrontends;
    private int taskNumber;

    private transient CopyContext copyContext;
    private boolean checkDirtyData = true;
    private final HologresRecordConverter<T, Record> recordConverter;

    public HologresJDBCCopyWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresRecordConverter<T, Record> converter,
            int numFrontends,
            int frontendOffset) {
        super(param, tableSchema);
        this.recordConverter = converter;
        this.numFrontends = numFrontends;
        this.frontendOffset = frontendOffset;
    }

    public static HologresJDBCCopyWriter<RowData> createRowDataWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresTableSchema hologresTableSchema,
            int numFrontends,
            int frontendOffset) {
        return new HologresJDBCCopyWriter<>(
                param,
                tableSchema,
                new HologresRowDataConverter<>(
                        tableSchema,
                        param,
                        new HologresJDBCRecordWriter(param),
                        new HologresJDBCRecordReader(
                                tableSchema.getFieldNames(), hologresTableSchema),
                        hologresTableSchema),
                numFrontends,
                frontendOffset);
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        LOG.info(
                "Initiating connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
        taskNumber = runtimeContext.getIndexOfThisSubtask();
        copyContext = new CopyContext();
        copyContext.init(param);
        LOG.info(
                "Successfully initiated connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
    }

    @Override
    public long writeAddRecord(T record) throws IOException {
        Record jdbcRecord = recordConverter.convertFrom(record);
        // The hologres instance cannot throw out dirty data details, so we do dirty data checking
        // on the holo-client. But there will be some performance loss, so we only do dirty data
        // check before the first flush, and no longer check after the first flush is successful.
        // When write fails, the job fail over recovery will re-register the writer.
        // At this time, the "checkDirtyData" will be true, that specific dirty data rows can be
        // thrown.
        if (checkDirtyData) {
            try {
                RecordChecker.check(jdbcRecord);
            } catch (HoloClientException e) {
                throw new IOException(
                        String.format(
                                "failed to copy because dirty data, the error record is %s.",
                                jdbcRecord),
                        e);
            }
        }
        jdbcRecord.setType(Put.MutationType.INSERT);
        try {
            if (copyContext.os == null) {
                boolean binary = "binary".equalsIgnoreCase(param.getCopyWriteFormat());
                com.alibaba.hologres.client.model.TableSchema schema = jdbcRecord.getSchema();
                copyContext.schema = schema;

                String sql =
                        CopyUtil.buildCopyInSql(
                                jdbcRecord,
                                binary,
                                param.getJDBCWriteMode() == INSERT_OR_IGNORE
                                        ? INSERT_OR_IGNORE
                                        : INSERT_OR_UPDATE);
                LOG.info("copy sql :{}", sql);
                CopyIn in = copyContext.manager.copyIn(sql);
                copyContext.os =
                        binary
                                ? new RecordBinaryOutputStream(
                                        new CopyInOutputStream(in),
                                        schema,
                                        copyContext.pgConn.unwrap(BaseConnection.class),
                                        1024 * 1024 * 10)
                                : new RecordTextOutputStream(
                                        new CopyInOutputStream(in),
                                        schema,
                                        copyContext.pgConn.unwrap(BaseConnection.class),
                                        1024 * 1024 * 10);
            }
            copyContext.os.putRecord(jdbcRecord);
        } catch (SQLException e) {
            LOG.error("close copyContext", e);
            copyContext.close();
            throw new IOException(e);
        }
        return jdbcRecord.getByteSize();
    }

    @Override
    public long writeDeleteRecord(T record) throws IOException {
        throw new IOException("jdbc copy mode does not support delete record");
    }

    @Override
    public void flush() throws IOException {
        try {
            if (copyContext.os != null) {
                copyContext.os.close();
            }
            checkDirtyData = false;
        } finally {
            copyContext.os = null;
        }
    }

    @Override
    public void close() {
        if (copyContext.os != null) {
            try {
                copyContext.os.close();
            } catch (IOException e) {
                LOG.warn("close fail", e);
            } finally {
                copyContext.os = null;
            }
        }
        copyContext.close();
        LOG.error("close copyContext");
    }

    class CopyContext {
        PgConnection pgConn;
        CopyManager manager;
        RecordOutputStream os = null;
        com.alibaba.hologres.client.model.TableSchema schema;

        public void init(HologresConnectionParam param) {
            try {
                Class.forName("com.alibaba.hologres.org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            Connection conn = null;
            String url = param.getJdbcOptions().getDbUrl();
            Properties info = new Properties();
            PGProperty.USER.set(info, param.getUsername());
            PGProperty.PASSWORD.set(info, param.getPassword());
            PGProperty.APPLICATION_NAME.set(info, "hologres-connector-flink_copy");
            try {
                // Copy is generally used in scenarios with large parallelism, but load balancing of
                // hologres vip endpoints may not be good. we randomize an offset and distribute
                // connections evenly to each fe
                if (numFrontends > 0) {
                    int choseFrontendId = chooseFrontendId();
                    LOG.info(
                            "taskNumber {}, number of frontends {}, frontend id offset {}, frontend id chose {}",
                            taskNumber,
                            numFrontends,
                            frontendOffset,
                            choseFrontendId);
                    url += ("?options=fe=" + choseFrontendId);
                    // for none public cloud, we connect to holo fe with inner ip:port directly
                    if (param.isCopyWriteDirectConnect()) {
                        String directUrl =
                                JDBCUtils.getJdbcDirectConnectionUrl(param.getJdbcOptions(), url);
                        try {
                            LOG.info("try connect directly to holo with url {}", directUrl);
                            conn = DriverManager.getConnection(directUrl, info);
                            LOG.info("init conn success with direct url {}", directUrl);
                        } catch (Exception e) {
                            LOG.warn("could not connect directly to holo.");
                        }
                    }
                }

                if (conn == null) {
                    LOG.info("init conn success to " + url);
                    conn = DriverManager.getConnection(url, info);
                }
                pgConn = conn.unwrap(PgConnection.class);
                LOG.info("init unwrap conn success");
                manager = new CopyManager(pgConn);
                LOG.info("init new manager success");
            } catch (SQLException e) {
                if (null != conn) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {

                    }
                }
                pgConn = null;
                manager = null;
                throw new RuntimeException(e);
            }
        }

        public void close() {
            manager = null;
            if (pgConn != null) {
                try {
                    pgConn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                pgConn = null;
            }
        }
    }

    private int chooseFrontendId() {
        // hologres frontend id starts from 1
        return (taskNumber + frontendOffset) % numFrontends + 1;
    }
}
