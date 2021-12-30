package com.alibaba.hologres.hive.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
            Configuration conf = taskAttemptContext.getConfiguration();

            // 目前使用holo-client不需要使用hive的split功能
            numSplits = 1;
            InputSplit[] splits = new InputSplit[numSplits];
            Path[] tablePaths = FileInputFormat.getInputPaths(jobConf);
            splits[0] = new HoloInputSplit(0, 0, tablePaths[0]);
            return splits;
        } catch (Exception e) {
            LOGGER.info("Error while splitting input data.", e);
            throw new IOException(e);
        }
    }
}
