package com.alibaba.hologres.hive.input;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * HoloInputFormat.
 */
public class HoloInputFormat extends HiveInputFormat<LongWritable, MapWritable> {
	/**
	 * {@inheritDoc}
	 */
	@Override
	public RecordReader<LongWritable, MapWritable>
	getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		throw new UnsupportedOperationException("Read operations are not allowed.");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		throw new UnsupportedOperationException("Read operations are not allowed.");
	}
}
