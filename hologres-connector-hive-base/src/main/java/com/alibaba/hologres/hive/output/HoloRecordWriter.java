package com.alibaba.hologres.hive.output;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.hive.HoloClientProvider;
import com.alibaba.hologres.hive.HoloRecordWritable;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** HoloRecordWriter. */
public class HoloRecordWriter implements FileSinkOperator.RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloRecordWriter.class);

    HoloClientProvider clientProvider;
    HoloClient client;
    TableSchema schema;

    public HoloRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        try {
            clientProvider = new HoloClientProvider(conf);
            client = clientProvider.createOrGetClient();
            schema = clientProvider.getTableSchema();
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
