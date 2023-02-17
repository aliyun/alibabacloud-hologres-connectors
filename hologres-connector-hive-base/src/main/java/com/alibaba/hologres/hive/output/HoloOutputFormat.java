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

import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.HoloClientProvider;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.hive.utils.JDBCUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/** HoloOutputFormat. */
public class HoloOutputFormat
        implements OutputFormat<NullWritable, MapWritable>,
                HiveOutputFormat<NullWritable, MapWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HoloOutputFormat.class);

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path,
            Class<? extends Writable> aClass,
            boolean b,
            Properties properties,
            Progressable progressable)
            throws IOException {
        TaskAttemptContext taskAttemptContext =
                ShimLoader.getHadoopShims().newTaskAttemptContext(jobConf, null);
        HoloClientParam param = new HoloClientParam(jobConf);
        HoloClientProvider clientProvider = new HoloClientProvider(param);
        try {
            TableSchema schema = clientProvider.getTableSchema();
            HoloVersion holoVersion;
            try {
                holoVersion =
                        clientProvider
                                .createOrGetClient()
                                .sql(ConnectionUtil::getHoloVersion)
                                .get();
            } catch (Exception e) {
                throw new IOException("Failed to get holo version", e);
            }
            boolean supportCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0;
            if (!supportCopy) {
                LOGGER.warn(
                        "The hologres instance version is {}, but only instances greater than 1.3.24 support copy write mode",
                        holoVersion);
            }
            if (param.isCopyWriteMode() && supportCopy) {
                try {
                    param.setHologresFrontendsNumber(
                            clientProvider
                                    .createOrGetClient()
                                    .sql(JDBCUtils::getFrontendsNumber)
                                    .get());
                } catch (Exception e) {
                    throw new IOException("Failed to get holo version", e);
                }
                if (param.isDirectConnect()) {
                    // 尝试直连，无法直连则各个tasks内的copy writer不需要进行尝试
                    param.setDirectConnect(JDBCUtils.couldDirectConnect(param));
                }
                return new HoloRecordCopyWriter(param, schema, taskAttemptContext);
            } else {
                return new HoloRecordWriter(param, taskAttemptContext);
            }
        } finally {
            clientProvider.closeClient();
        }
    }

    @Override
    public RecordWriter<NullWritable, MapWritable> getRecordWriter(
            FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
            throws IOException {
        throw new UnsupportedOperationException("Write operations are not allowed.");
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {}
}
