package com.alibaba.ververica.connectors.hologres.jdbc.copy;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.copy.in.CopyInOutputStream;
import com.alibaba.hologres.client.copy.in.RecordBinaryOutputStream;
import com.alibaba.hologres.client.copy.in.RecordOutputStream;
import com.alibaba.hologres.client.copy.in.RecordTextOutputStream;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.utils.IdentifierUtil;
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
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCClientProvider;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCRecordReader;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCRecordWriter;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static com.alibaba.hologres.client.copy.CopyUtil.buildCopyInSql;
import static com.alibaba.hologres.client.model.OnConflictAction.INSERT_OR_IGNORE;
import static com.alibaba.hologres.client.model.OnConflictAction.INSERT_OR_UPDATE;
import static com.alibaba.ververica.connectors.hologres.utils.JDBCUtils.executeSql;

/** An IO writer implementation for JDBC. */
public class HologresJDBCCopyWriter<T> extends HologresWriter<T> {
    private static final transient Logger LOG =
            LoggerFactory.getLogger(HologresJDBCCopyWriter.class);
    private final int frontendOffset;
    private final int numFrontends;
    private int taskNumber;
    private int numTasks;
    private final int shardCount;
    private final CopyMode copyMode;
    private transient Map<String, CopyContext> partitionValueToCopyContext;
    private transient Map<String, com.alibaba.hologres.client.model.TableSchema>
            partitionValueToTableSchema;
    private boolean checkDirtyData = true;
    private final HologresRecordConverter<T, Record> recordConverter;
    private transient HologresJDBCClientProvider clientProvider;

    public HologresJDBCCopyWriter(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresRecordConverter<T, Record> converter,
            int numFrontends,
            int frontendOffset,
            int shardCount) {
        super(param, fieldNames, fieldTypes);
        this.recordConverter = converter;
        this.numFrontends = numFrontends;
        this.frontendOffset = frontendOffset;
        this.copyMode = param.getCopyMode();
        this.shardCount = shardCount;
    }

    public static HologresJDBCCopyWriter<RowData> createRowDataWriter(
            HologresConnectionParam param,
            String[] fieldNames,
            LogicalType[] fieldTypes,
            HologresTableSchema hologresTableSchema,
            int numFrontends,
            int frontendOffset) {
        return new HologresJDBCCopyWriter<>(
                param,
                fieldNames,
                fieldTypes,
                new HologresRowDataConverter<>(
                        fieldNames,
                        fieldTypes,
                        param,
                        new HologresJDBCRecordWriter(param),
                        new HologresJDBCRecordReader(fieldNames, hologresTableSchema),
                        hologresTableSchema),
                numFrontends,
                frontendOffset,
                hologresTableSchema.getShardCount());
    }

    @Override
    public void open(Integer taskNumber, Integer numTasks) {
        LOG.info(
                "Initiating connection to database [{}] / table[{}], whole connection params: {}",
                param.getJdbcOptions().getDatabase(),
                param.getTable(),
                param);
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        partitionValueToCopyContext = new HashMap<>();
        partitionValueToTableSchema = new HashMap<>();
        this.clientProvider = new HologresJDBCClientProvider(param);
        LOG.info(
                "Successfully initiated connection to database [{}] / table[{}]",
                param.getJdbcOptions().getDatabase(),
                param.getTable());
    }

    @Override
    public long writeAddRecord(T record) throws IOException {
        CopyContext copyContext;
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
        com.alibaba.hologres.client.model.TableSchema schema = jdbcRecord.getSchema();
        if (schema.isPartitionParentTable()) {
            String partitionValue =
                    String.valueOf(jdbcRecord.getObject(schema.getPartitionIndex()));
            if (partitionValueToCopyContext.containsKey(partitionValue)) {
                copyContext = partitionValueToCopyContext.get(partitionValue);
            } else {
                com.alibaba.hologres.client.model.TableSchema childSchema =
                        checkChildTableExists(schema, partitionValue);
                partitionValueToTableSchema.put(partitionValue, childSchema);
                copyContext = new CopyContext();
                copyContext.init(param);
                copyContext.schema = childSchema;
                partitionValueToCopyContext.put(partitionValue, copyContext);
                if (partitionValueToCopyContext.size() > 5) {
                    throw new RuntimeException(
                            "Only support to write less than 5 child table at the same time now.");
                }
            }
            jdbcRecord.changeToChildSchema(partitionValueToTableSchema.get(partitionValue));
        } else {
            copyContext =
                    partitionValueToCopyContext.computeIfAbsent(
                            param.getTable(), k -> new CopyContext().init(param));
        }

        try {
            if (copyContext.os == null) {
                boolean binary = "binary".equalsIgnoreCase(param.getCopyWriteFormat());
                String sql =
                        buildCopyInSql(
                                jdbcRecord,
                                binary,
                                param.getJDBCWriteMode() == INSERT_OR_IGNORE
                                        ? INSERT_OR_IGNORE
                                        : INSERT_OR_UPDATE,
                                copyMode);
                LOG.info("copy sql :{}", sql);
                CopyIn in = copyContext.manager.copyIn(sql);
                copyContext.ios = new CopyInOutputStream(in);
                // holo bulk load copy just support text, not support binary
                if (copyMode != CopyMode.STREAM) {
                    copyContext.os =
                            new RecordTextOutputStream(
                                    copyContext.ios,
                                    schema,
                                    copyContext.pgConn.unwrap(BaseConnection.class),
                                    1024 * 1024 * 10);
                } else {
                    copyContext.os =
                            binary
                                    ? new RecordBinaryOutputStream(
                                            copyContext.ios,
                                            schema,
                                            copyContext.pgConn.unwrap(BaseConnection.class),
                                            1024 * 1024 * 10)
                                    : new RecordTextOutputStream(
                                            copyContext.ios,
                                            schema,
                                            copyContext.pgConn.unwrap(BaseConnection.class),
                                            1024 * 1024 * 10);
                }
            }
            copyContext.os.putRecord(jdbcRecord);
            if (param.isEnableAggressive() && copyMode == CopyMode.STREAM) {
                copyContext.ios.flush();
            }
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
            for (CopyContext copyContext : partitionValueToCopyContext.values()) {
                if (copyContext.os != null) {
                    copyContext.os.close();
                } else {
                    copyContext.active = false;
                }
            }
            checkDirtyData = false;
        } finally {
            Iterator<Map.Entry<String, CopyContext>> iterator =
                    partitionValueToCopyContext.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, CopyContext> entry = iterator.next();
                CopyContext copyContext = entry.getValue();
                if (copyContext.active) {
                    copyContext.os = null;
                } else {
                    copyContext.close();
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public void close() {
        for (Map.Entry<String, CopyContext> entry : partitionValueToCopyContext.entrySet()) {
            CopyContext copyContext = entry.getValue();
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
            LOG.error("close copyContext for {}", entry.getKey());
        }
        LOG.error("close all copyContext");
        if (clientProvider != null) {
            clientProvider.closeClient();
        }
        partitionValueToCopyContext.clear();
        partitionValueToTableSchema.clear();
    }

    class CopyContext {
        PgConnection pgConn;
        CopyManager manager;
        CopyInOutputStream ios = null;
        RecordOutputStream os = null;
        com.alibaba.hologres.client.model.TableSchema schema;
        boolean active;

        public CopyContext init(HologresConnectionParam param) {
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
                                param.getJdbcOptions(),
                                url, /*sslModeConnection*/
                                true, /*maxRetryCount*/
                                3, /*appName*/
                                "hologres-connector-flink-" + copyMode);
                LOG.info("init conn success to fe " + url);
                pgConn = conn.unwrap(PgConnection.class);
                LOG.info("init unwrap conn success");
                // Set statement_timeout at the session level，avoid being affected by db level
                // configuration. (for example, less than the checkpoint time)
                executeSql(
                        pgConn,
                        String.format(
                                "set statement_timeout = %s", param.getStatementTimeoutSeconds()));
                if (param.isEnableServerlessComputing()) {
                    executeSql(pgConn, "set hg_computing_resource = 'serverless';");
                    executeSql(
                            pgConn,
                            String.format(
                                    "set hg_experimental_serverless_computing_query_priority = '%d';",
                                    param.getServerlessComputingQueryPriority()));
                }
                // 不抛出异常: copy不需要返回影响行数所以默认关闭,但此guc仅部分版本支持,而且设置失败不影响程序运行
                executeSql(
                        pgConn,
                        "set hg_experimental_enable_fixed_dispatcher_affected_rows = off;",
                        false);
                if (param.isEnableReshuffleByHolo()) {
                    String result =
                            getTargetShardList().stream()
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(","));
                    LOG.info(
                            "enable target-shards, numTasks: {}, this taskNumber: {}, target shard list: {}",
                            numTasks,
                            taskNumber,
                            result);
                    executeSql(
                            pgConn,
                            String.format("set hg_experimental_target_shard_list = '%s'", result));
                }
                if (copyMode.equals(CopyMode.BULK_LOAD_ON_CONFLICT)) {
                    executeSql(pgConn, "set hg_experimental_copy_enable_on_conflict = on;", true);
                    executeSql(
                            pgConn,
                            "set hg_experimental_affect_row_multiple_times_keep_last = on;",
                            true);
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
            return this;
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

    /**
     * compute target shard list for re-shuffle.
     *
     * <p>If the holo table has Distribution Keys, data will only be written to the first shardCount
     * tasks after reshuffle. In other words, if the number of tasks is greater than shardCount,
     * there will be no data flow into the additional tasks. If the holo table does not have
     * Distribution Keys set, reshuffle will be random, and all tasks will have traffic and only
     * write to a single shard.
     *
     * <p>for example, hava distribution key, numTasks = 2, shardCount = 4: Task 0: [0, 2], Task 1:
     * [1, 3]
     *
     * <p>for example, hava distribution key, numTasks = 8, shardCount = 4: Task 0: [0], Task 1:
     * [1], Task 2: [2], Task 3: [3], Task 4-7: none
     *
     * <p>for example, no distribution key, numTasks = 8, shardCount = 4: Task 0: [0], Task 1: [1],
     * Task 2: [2], Task 3: [3], Task 4-7: random single shard
     *
     * @return target shard list
     */
    private List<Integer> getTargetShardList() {
        List<Integer> targetShardList = new ArrayList<>();
        for (int i = 0; i * numTasks + taskNumber < shardCount; i++) {
            int p = i * numTasks + taskNumber;
            targetShardList.add(p);
        }
        if (targetShardList.isEmpty()) {
            targetShardList.add(new Random().nextInt(shardCount));
        }
        return targetShardList;
    }

    private com.alibaba.hologres.client.model.TableSchema checkChildTableExists(
            com.alibaba.hologres.client.model.TableSchema schema, String partitionValue) {
        boolean isStr =
                Types.VARCHAR == schema.getColumn(schema.getPartitionIndex()).getType()
                        || Types.DATE == schema.getColumn(schema.getPartitionIndex()).getType();
        try {
            Partition partition =
                    clientProvider
                            .getClient()
                            .sql(
                                    conn -> {
                                        Partition p =
                                                ConnectionUtil.getPartition(
                                                        conn,
                                                        schema.getSchemaName(),
                                                        schema.getTableName(),
                                                        partitionValue,
                                                        isStr);
                                        if (p == null) {
                                            throw new RuntimeException(
                                                    String.format(
                                                            "child table for partition value %s does not exist",
                                                            partitionValue));
                                        }
                                        return p;
                                    })
                            .get();

            return clientProvider
                    .getClient()
                    .getTableSchema(
                            TableName.valueOf(
                                    IdentifierUtil.quoteIdentifier(partition.getSchemaName(), true),
                                    IdentifierUtil.quoteIdentifier(partition.getTableName(), true)),
                            false);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
