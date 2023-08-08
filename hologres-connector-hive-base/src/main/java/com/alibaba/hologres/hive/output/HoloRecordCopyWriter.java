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
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_IGNORE;
import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_UPDATE;
import static com.alibaba.hologres.hive.utils.JDBCUtils.logErrorAndExceptionInConsole;

/** HoloRecordWriter. */
public class HoloRecordCopyWriter implements FileSinkOperator.RecordWriter {

    private static final Logger logger = LoggerFactory.getLogger(HoloRecordCopyWriter.class);
    private final int maxWriterNumberPerTask;

    private ScheduledExecutorService backgroundExecutorService;
    private final AtomicInteger nextCopyContextIndex;
    private final transient ConcurrentMap<Integer, CopyContext> copyContexts;
    private final int maxWriterNumber;

    private final boolean binary;
    private final HoloClientParam param;
    private final TableSchema schema;
    private final String appName;
    private long count = 0;

    public HoloRecordCopyWriter(
            HoloClientParam param, TableSchema schema, TaskAttemptContext context)
            throws IOException {
        this.maxWriterNumberPerTask = param.getMaxWriterNumberPerTask();
        this.param = param;
        this.binary = "binary".equals(param.getCopyWriteFormat());
        this.schema = schema;
        this.appName = String.format("hologres-connector-hive_copy_%s", context.getJobID());
        CopyContext copyContext = new CopyContext();
        copyContext.init(param, appName);
        this.nextCopyContextIndex = new AtomicInteger(0);
        this.copyContexts = new ConcurrentHashMap<>(1);
        this.copyContexts.put(nextCopyContextIndex.getAndIncrement(), copyContext);
        this.maxWriterNumber = param.getMaxWriterNumber();
        // 当此参数大于0时，会适当增加每个task的copy writer数量，每个task不超过5
        if (maxWriterNumber > 0) {
            this.backgroundExecutorService = Executors.newSingleThreadScheduledExecutor();
            this.backgroundExecutorService.scheduleAtFixedRate(
                    this::checkIfNeedIncreaseCopyContexts,
                    new Random().nextInt(60),
                    60,
                    TimeUnit.SECONDS);
        }
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
            ++count;
            if (param.isDirtyDataCheck()) {
                try {
                    RecordChecker.check(record);
                } catch (HoloClientException e) {
                    logErrorAndExceptionInConsole(
                            String.format(
                                    "failed to copy because dirty data, the error record is %s.",
                                    record),
                            e);
                    throw new IOException(
                            String.format(
                                    "failed to copy because dirty data, the error record is %s.",
                                    record),
                            e);
                }
            }
            // nextCopyContextIndex既是map的size
            writeWithCopyContext(
                    record, copyContexts.get((int) (count % nextCopyContextIndex.get())));
        } catch (HiveHoloStorageException e) {
            logErrorAndExceptionInConsole(
                    String.format(
                            "failed while write values %s, because:",
                            Arrays.toString(recordWritable.getColumnValues())),
                    e);
            if (copyContexts != null) {
                for (CopyContext copyContext : copyContexts.values()) {
                    if (copyContext != null) {
                        copyContext.close();
                    }
                }
            }
            throw new IOException(e);
        }
    }

    private void writeWithCopyContext(Record record, CopyContext copyContext) throws IOException {
        try {
            if (copyContext.os == null) {
                TableSchema schema = record.getSchema();
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
        } catch (SQLException e) {
            logErrorAndExceptionInConsole(
                    String.format("failed while writeWithCopyContext record %s, because:", record),
                    e);
            copyContext.close();
            throw new IOException(e);
        }
    }

    @Override
    public void close(boolean b) throws IOException {
        if (backgroundExecutorService != null) {
            backgroundExecutorService.shutdown();
        }
        if (copyContexts != null) {
            for (CopyContext copyContext : copyContexts.values()) {
                if (copyContext != null) {
                    copyContext.close();
                }
            }
        }
    }

    private void checkIfNeedIncreaseCopyContexts() {
        if (copyContexts.size() >= maxWriterNumberPerTask) {
            return;
        }
        // 当前以jobId为application_name的连接数
        int connectionsNumber = JDBCUtils.getConnectionsNumberOfThisJob(param, appName);
        // 当前连接数小于最大连接数，可以适当增加copy writer数量，避免长尾
        if (probabilityIncrease(connectionsNumber)) {
            CopyContext temp = new CopyContext();
            temp.init(param, appName);
            copyContexts.put(nextCopyContextIndex.get(), temp);
            // 新的copy context添加到map之后，再对index进行加一
            if (nextCopyContextIndex.incrementAndGet() != copyContexts.size()) {
                throw new RuntimeException(
                        String.format(
                                "should not happened, the size of copyContexts %s not equals to nextCopyContextIndex %s",
                                copyContexts.size(), nextCopyContextIndex.incrementAndGet()));
            }
            logger.info(
                    "create new enhance copy contexts, current number is {}",
                    nextCopyContextIndex.get());
        }
    }

    private boolean probabilityIncrease(int connectionsNumber) {
        if (maxWriterNumber <= connectionsNumber) {
            return false;
        }
        // 防止多个task同时创建新连接超出最大连接数限制，可用的空闲连接数越大，创建的概率越大
        double rate = (double) (maxWriterNumber - connectionsNumber) / (connectionsNumber + 1);
        return new Random().nextInt(100) < rate * 100;
    }

    static class CopyContext {
        PgConnection pgConn;
        CopyManager manager;
        RecordOutputStream os = null;
        com.alibaba.hologres.client.model.TableSchema schema;

        public void init(HoloClientParam param, String appName) {
            Connection conn = null;
            String url = param.getUrl();
            Properties info = new Properties();
            PGProperty.USER.set(info, param.getUsername());
            PGProperty.PASSWORD.set(info, param.getPassword());
            PGProperty.APPLICATION_NAME.set(info, appName);

            try {
                // copy write mode 的瓶颈往往是vip endpoint的网络吞吐，因此我们在可以直连holo fe的场景默认使用直连
                if (param.isDirectConnect()) {
                    String directUrl = JDBCUtils.getJdbcDirectConnectionUrl(param);
                    try {
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
            logger.info("close copyContext");
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    logErrorAndExceptionInConsole("close RecordOutputStream fail", e);
                    throw new RuntimeException(e);
                } finally {
                    os = null;
                }
            }
            manager = null;
            if (pgConn != null) {
                try {
                    pgConn.close();
                } catch (SQLException e) {
                    logErrorAndExceptionInConsole("close pg Conn fail", e);
                    throw new RuntimeException(e);
                }
                pgConn = null;
            }
        }
    }
}
