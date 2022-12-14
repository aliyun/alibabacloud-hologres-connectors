package com.alibaba.hologres.hive.output;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.HoloClientProvider;
import com.alibaba.hologres.hive.HoloRecordWritable;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** HoloRecordWriter. */
public class HoloRecordWriter implements FileSinkOperator.RecordWriter {

    private static final Logger logger = LoggerFactory.getLogger(HoloRecordWriter.class);

    HoloClientProvider clientProvider;
    TableSchema schema;

    public HoloRecordWriter(HoloClientParam param, TaskAttemptContext context) throws IOException {
        clientProvider = new HoloClientProvider(param);
        schema = clientProvider.getTableSchema();
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
            clientProvider.createOrGetClient().put(put);
        } catch (HiveHoloStorageException | HoloClientException throwables) {
            clientProvider.closeClient();
            throw new IOException(throwables);
        }
    }

    @Override
    public void close(boolean b) {
        if (clientProvider != null) {
            clientProvider.closeClient();
        }
    }
}
