package com.alibaba.hologres.hive.output;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.hologres.hive.HoloRecordWritable;
import com.alibaba.hologres.hive.conf.HoloStorageConfig;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.postgresql.model.TableName;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** HoloRecordWriter. */
public class HoloRecordWriter implements FileSinkOperator.RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloRecordWriter.class);

    HoloClient client;
    TableSchema schema;

    public HoloRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get(HoloStorageConfig.TABLE.getPropertyName());
        String url = conf.get(HoloStorageConfig.JDBC_URL.getPropertyName());
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Url should be defined");
        }

        String username = conf.get(HoloStorageConfig.USERNAME.getPropertyName());
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username should be defined");
        }

        String password = conf.get(HoloStorageConfig.PASSWORD.getPropertyName());
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("password should be defined");
        }

        WriteMode writeMode;
        String wMode =
                conf.get(HoloStorageConfig.WRITE_MODE.getPropertyName(), "INSERT_OR_REPLACE")
                        .toLowerCase();
        switch (wMode) {
            case "insert_or_replace":
                writeMode = WriteMode.INSERT_OR_REPLACE;
                break;
            case "insert_or_ignore":
                writeMode = WriteMode.INSERT_OR_IGNORE;
                break;
            case "insert_or_update":
                writeMode = WriteMode.INSERT_OR_UPDATE;
                break;
            default:
                throw new IllegalArgumentException("Could not recognize writeMode " + wMode);
        }
        WriteFailStrategy writeFailStrategy;
        String wFailStrategy =
                conf.get(HoloStorageConfig.WRITE_FAIL_STRATEGY.getPropertyName(), "TRY_ONE_BY_ONE")
                        .toLowerCase();
        switch (wFailStrategy) {
            case "try_one_by_one":
                writeFailStrategy = WriteFailStrategy.TRY_ONE_BY_ONE;
                break;
            case "none":
                writeFailStrategy = WriteFailStrategy.NONE;
                break;
            default:
                throw new IllegalArgumentException(
                        "Could not recognize writeFailStrategy " + wFailStrategy);
        }

        int writeBatchSize = conf.getInt(HoloStorageConfig.WRITE_BATCH_SIZE.getPropertyName(), 512);
        long writeBatchByteSize =
                conf.getLong(
                        HoloStorageConfig.WRITE_BATCH_BYTE_SIZE.getPropertyName(),
                        2L * 1024L * 1024L);
        int rewriteSqlMaxBatchSize =
                conf.getInt(HoloStorageConfig.REWRITE_SQL_MAX_BATCH_SIZE.getPropertyName(), 1024);

        long writeMaxIntervalMs =
                conf.getLong(HoloStorageConfig.WRITE_MAX_INTERVAL_MS.getPropertyName(), 10000L);
        int writeThreadSize = conf.getInt(HoloStorageConfig.WRITE_THREAD_SIZE.getPropertyName(), 1);
        int retryCount = conf.getInt(HoloStorageConfig.RETRY_COUNT.getPropertyName(), 3);
        long retrySleepInitMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_INIT_MS.getPropertyName(), 1000L);
        long retrySleepStepMs =
                conf.getLong(HoloStorageConfig.RETRY_SLEEP_STEP_MS.getPropertyName(), 10000L);
        long connectionMaxIdleMs =
                conf.getLong(HoloStorageConfig.CONNECTION_MAX_IDLE_MS.getPropertyName(), 60000L);

        try {
            HoloConfig holoConfig = new HoloConfig();
            holoConfig.setWriteMode(writeMode);
            holoConfig.setWriteFailStrategy(writeFailStrategy);
            holoConfig.setWriteBatchSize(writeBatchSize);
            holoConfig.setWriteBatchByteSize(writeBatchByteSize);
            holoConfig.setRewriteSqlMaxBatchSize(rewriteSqlMaxBatchSize);
            holoConfig.setWriteMaxIntervalMs(writeMaxIntervalMs);
            holoConfig.setWriteThreadSize(writeThreadSize);
            holoConfig.setRetryCount(retryCount);
            holoConfig.setRetrySleepInitMs(retrySleepInitMs);
            holoConfig.setRetrySleepStepMs(retrySleepStepMs);
            holoConfig.setConnectionMaxIdleMs(connectionMaxIdleMs);

            holoConfig.setJdbcUrl(url);
            holoConfig.setUsername(username);
            holoConfig.setPassword(password);
            client = new HoloClient(holoConfig);
            schema = client.getTableSchema(TableName.valueOf(tableName));
            client.setAsyncCommit(true);
        } catch (HoloClientException e) {
            close(true);
            throw new IOException(e);
        }
    }

    public static String getPasswdFromKeystore(String keystore, String key) throws IOException {
        String passwd = null;
        if (keystore != null && key != null) {
            Configuration conf = new Configuration();
            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystore);
            char[] pwdCharArray = conf.getPassword(key);
            if (pwdCharArray != null) {
                passwd = new String(pwdCharArray);
            }
        }
        return passwd;
    }

    @Override
    public void write(Writable writable) throws IOException {

        if (!(writable instanceof HoloRecordWritable)) {
            throw new IOException(
                    "Expected HoloRecordWritable. Got " + writable.getClass().getName());
        }
        HoloRecordWritable record = (HoloRecordWritable) writable;
        try {
            Put put = new Put(schema);
            record.write(put);
            client.put(put);
        } catch (HiveHoloStorageException | HoloClientException throwables) {
            throw new IOException(throwables);
        }
    }

    @Override
    public void close(boolean b) throws IOException {
        if (client != null) {
            try {
                client.flush();
            } catch (HoloClientException throwables) {
                throw new IOException(throwables);
            }
            client.close();
        }
    }
}
