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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_IGNORE;
import static com.alibaba.hologres.client.model.WriteMode.INSERT_OR_UPDATE;

/** An IO writer implementation for JDBC. */
public class HologresJDBCCopyWriter<T> extends HologresWriter<T> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresJDBCCopyWriter.class);
    private final int frontendOffset;
    private final int numFrontends;
    private int taskNumber;
    private int numTasks;
    private final int shardCount;
    private final boolean bulkLoad;
    private transient CopyContext copyContext;
    private boolean checkDirtyData = true;
    private final HologresRecordConverter<T, Record> recordConverter;

    public HologresJDBCCopyWriter(
            HologresConnectionParam param,
            TableSchema tableSchema,
            HologresRecordConverter<T, Record> converter,
            int numFrontends,
            int frontendOffset,
            int shardCount) {
        super(param, tableSchema);
        this.recordConverter = converter;
        this.numFrontends = numFrontends;
        this.frontendOffset = frontendOffset;
        this.bulkLoad = param.isBulkLoad();
        this.shardCount = shardCount;
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
                frontendOffset,
                hologresTableSchema.getShardCount());
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        LOG.info(
                "Initiating connection to database [{}] / table[{}], whole connection params: {}",
                param.getJdbcOptions().getDatabase(),
                param.getTable(),
                param);
        taskNumber = runtimeContext.getIndexOfThisSubtask();
        numTasks = runtimeContext.getNumberOfParallelSubtasks();
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
        LOG.debug("Hologres insert record in JDBC-COPY: {}", jdbcRecord);
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
                                        : INSERT_OR_UPDATE,
                                !bulkLoad);
                LOG.info("copy sql :{}", sql);
                CopyIn in = copyContext.manager.copyIn(sql);
                // holo bulk load copy just support text, not support binary
                if (bulkLoad) {
                    copyContext.os =
                            new RecordTextOutputStream(
                                    new CopyInOutputStream(in),
                                    schema,
                                    copyContext.pgConn.unwrap(BaseConnection.class),
                                    1024 * 1024 * 10);
                } else {
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
            Connection conn = null;
            String url = param.getJdbcOptions().getDbUrl();
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
                if (param.isDirectConnect()) {
                    url = JDBCUtils.getJdbcDirectConnectionUrl(param.getJdbcOptions(), url);
                    LOG.info("will connect directly to fe id {} with url {}", choseFrontendId, url);
                }
            }
            try {
                conn =
                        JDBCUtils.createConnection(
                                param.getJdbcOptions(), url, /*sslModeConnection*/ true);
                LOG.info("init conn success to fe " + url);
                pgConn = conn.unwrap(PgConnection.class);
                LOG.info("init unwrap conn success");
                // Set statement_timeout at the session level，avoid being affected by db level
                // configuration.
                // (for example, less than the checkpoint time)
                try (Statement stat = pgConn.createStatement()) {
                    stat.execute("set statement_timeout = '8h';");
                } catch (SQLException e) {
                    throw new RuntimeException("set statement_timeout to 8h failed because", e);
                }
                try (Statement stat = pgConn.createStatement()) {
                    stat.execute(
                            "set hg_experimental_enable_fixed_dispatcher_affected_rows = off;");
                } catch (SQLException ignored) {
                    // 不抛出异常: copy不需要返回影响行数所以默认关闭,但此guc仅部分版本支持,而且设置失败不影响程序运行
                    LOG.warn(
                            "set hg_experimental_enable_fixed_dispatcher_affected_rows failed because ",
                            ignored);
                }
                if (param.isEnableTargetShards()) {
                    String result =
                            getTargetShardList().stream()
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(","));
                    LOG.info(
                            "enable target-shards, numTasks: {}, this taskNumber: {}, target shard list: {}",
                            numTasks,
                            taskNumber,
                            result);
                    try (Statement stat = pgConn.createStatement()) {
                        stat.execute(
                                String.format(
                                        "set hg_experimental_target_shard_list = '%s'", result));
                    } catch (SQLException e) {
                        throw new RuntimeException(
                                "set hg_experimental_target_shard_list failed because ", e);
                    }
                }
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

    private List<Integer> getTargetShardList() {
        List<Integer> targetShardList = new ArrayList<>();
        for (int i = 0; i * numTasks + taskNumber <= shardCount; i++) {
            int p = i * numTasks + taskNumber;
            targetShardList.add(p);
        }
        return targetShardList;
    }
}
