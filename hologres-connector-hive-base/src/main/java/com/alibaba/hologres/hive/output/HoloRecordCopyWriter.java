package com.alibaba.hologres.hive.output;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.copy.CopyInOutputStream;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.RecordOutputStream;
import com.alibaba.hologres.client.copy.RecordTextOutputStream;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.RecordChecker;
import com.alibaba.hologres.hive.HoloRecordWritable;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import com.alibaba.hologres.hive.utils.JDBCUtils;
import com.alibaba.hologres.org.postgresql.PGProperty;
import com.alibaba.hologres.org.postgresql.copy.CopyIn;
import com.alibaba.hologres.org.postgresql.copy.CopyManager;
import com.alibaba.hologres.org.postgresql.core.BaseConnection;
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_IGNORE;
import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_UPDATE;

/** HoloRecordWriter. */
public class HoloRecordCopyWriter implements FileSinkOperator.RecordWriter {

    private static final Logger logger = LoggerFactory.getLogger(HoloRecordCopyWriter.class);
    private final transient CopyContext copyContext;
    private final boolean binary;
    private final HoloClientParam param;
    private final TableSchema schema;

    public HoloRecordCopyWriter(
            HoloClientParam param, TableSchema schema, TaskAttemptContext context)
            throws IOException {
        this.param = param;
        this.binary = "binary".equals(param.getCopyWriteFormat());
        this.schema = schema;
        this.copyContext = new CopyContext();
        this.copyContext.init(param);
    }

    @Override
    public void write(Writable writable) throws IOException {

        if (!(writable instanceof HoloRecordWritable)) {
            throw new IOException(
                    "Expected HoloRecordWritable. Got " + writable.getClass().getName());
        }
        HoloRecordWritable recordWritable = (HoloRecordWritable) writable;

        try {
            Put put = new Put(schema);
            recordWritable.write(put);
            Record record = put.getRecord();
            if (param.isCopyWriteDirtyDataCheck()) {
                try {
                    RecordChecker.check(record);
                } catch (HoloClientException e) {
                    throw new IOException(
                            String.format(
                                    "failed to copy because dirty data, the error record is %s.",
                                    record),
                            e);
                }
            }
            if (copyContext.os == null) {
                com.alibaba.hologres.client.model.TableSchema schema = record.getSchema();
                copyContext.schema = schema;

                String sql =
                        CopyUtil.buildCopyInSql(
                                record,
                                binary,
                                param.getWriteMode() == INSERT_OR_IGNORE
                                        ? INSERT_OR_IGNORE
                                        : INSERT_OR_UPDATE);
                logger.info("copy sql :{}", sql);
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

            copyContext.os.putRecord(record);
        } catch (HiveHoloStorageException | SQLException e) {
            logger.error("close copyContext", e);
            copyContext.close();
            throw new IOException(e);
        }
    }

    @Override
    public void close(boolean b) throws IOException {
        if (copyContext.os != null) {
            try {
                copyContext.os.close();
            } catch (IOException e) {
                logger.warn("close fail", e);
                throw new IOException(e);
            } finally {
                copyContext.os = null;
            }
        }
        copyContext.close();
        logger.error("close copyContext");
    }

    static class CopyContext {
        PgConnection pgConn;
        CopyManager manager;
        RecordOutputStream os = null;
        com.alibaba.hologres.client.model.TableSchema schema;

        public void init(HoloClientParam param) {
            Connection conn = null;
            String url = param.getUrl();
            Properties info = new Properties();
            PGProperty.USER.set(info, param.getUsername());
            PGProperty.PASSWORD.set(info, param.getPassword());
            PGProperty.APPLICATION_NAME.set(info, "hologres-connector-hive_copy");

            try {
                // copy write mode 的瓶颈往往是vip endpoint的网络吞吐，因此我们在可以直连holo fe的场景默认使用直连
                if (param.isCopyWriteDirectConnect()) {
                    String directUrl = JDBCUtils.getJdbcDirectConnectionUrl(param);
                    try {
                        logger.info("try connect directly to holo with url {}", directUrl);
                        conn = DriverManager.getConnection(directUrl, info);
                        logger.info("init conn success with direct url {}", directUrl);
                    } catch (Exception e) {
                        logger.warn("could not connect directly to holo.");
                    }
                }
                if (conn == null) {
                    logger.info("init conn success to " + url);
                    conn = DriverManager.getConnection(url, info);
                }
                pgConn = conn.unwrap(PgConnection.class);
                logger.info("init unwrap conn success");
                manager = new CopyManager(pgConn);
                logger.info("init new manager success");
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
}
