package com.alibaba.hologres.hive.input;

import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.HoloClientProvider;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.alibaba.hologres.client.Command.getShardCount;

/** HoloInputFormat. */
public class HoloInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloInputFormat.class);

    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(
            InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        if (!(inputSplit instanceof HoloInputSplit)) {
            throw new RuntimeException(
                    "Incompatible split type " + inputSplit.getClass().getName() + ".");
        }
        TaskAttemptContext taskAttemptContext =
                ShimLoader.getHadoopShims().newTaskAttemptContext(jobConf, null);
        return new HoloRecordReader(taskAttemptContext, (HoloInputSplit) inputSplit);
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        try {
            TaskAttemptContext taskAttemptContext =
                    ShimLoader.getHadoopShims().newTaskAttemptContext(jobConf, null);
            HoloClientParam param = new HoloClientParam(jobConf);
            HoloClientProvider clientProvider = new HoloClientProvider(param);
            try {
                TableSchema schema = clientProvider.getTableSchema();
                int shardCount = getShardCount(clientProvider.createOrGetClient(), schema);

                int size = shardCount / numSplits;
                int remain = shardCount % numSplits;

                InputSplit[] splits = new InputSplit[numSplits];
                int start = 0;
                for (int i = 0; i < numSplits; i++) {
                    int end;
                    if (remain > 0) {
                        end = start + size + 1;
                        remain--;
                    } else {
                        end = start + size;
                    }
                    splits[i] = new HoloInputSplit(start, end, schema);
                    start = end;
                }
                return splits;
            } finally {
                clientProvider.closeClient();
            }

        } catch (Exception e) {
            LOGGER.info("Error while splitting input data.", e);
            throw new IOException(e);
        }
    }
}
