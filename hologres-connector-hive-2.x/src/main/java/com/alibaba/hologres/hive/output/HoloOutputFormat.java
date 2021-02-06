/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.hologres.hive.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * HoloOutputFormat.
 */
public class HoloOutputFormat implements OutputFormat<NullWritable, MapWritable>,
		HiveOutputFormat<NullWritable, MapWritable> {

	public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class<? extends Writable> aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
		TaskAttemptContext taskAttemptContext = ShimLoader.getHadoopShims().newTaskAttemptContext(jobConf, null);
		return new HoloRecordWriter(taskAttemptContext);
	}

	public RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
		throw new UnsupportedOperationException("Write operations are not allowed.");
	}

	public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

	}

}
