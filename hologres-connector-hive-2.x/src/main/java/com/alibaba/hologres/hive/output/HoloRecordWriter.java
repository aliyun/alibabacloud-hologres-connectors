package com.alibaba.hologres.hive.output;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
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
    String url;
    HoloClient client;
    String tableName;

    TableSchema schema;

    public HoloRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        tableName = conf.get(HoloStorageConfig.TABLE.getPropertyName());
        url = conf.get(HoloStorageConfig.JDBC_URL.getPropertyName());
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Url should be defined");
        }

        String username = conf.get(HoloStorageConfig.USERNAME.getPropertyName());

        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }
        String password = conf.get(HoloStorageConfig.PASSWORD.getPropertyName());

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }

        int batchSize = conf.getInt(HoloStorageConfig.JDBC_WRITE_SIZE.getPropertyName(), 512);

        long batchByteSize =
                conf.getLong(
                        HoloStorageConfig.JDBC_WRITE_BYTE_SIZE.getPropertyName(),
                        100L * 1024L * 1024L);

        LOGGER.info("batchSize:{}", batchSize);
        LOGGER.info("batchByteSize:{}", batchByteSize);
        try {
            HoloConfig holoConfig = new HoloConfig();
            holoConfig.setWriteBatchSize(batchSize);
            holoConfig.setWriteBatchByteSize(batchByteSize);
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
