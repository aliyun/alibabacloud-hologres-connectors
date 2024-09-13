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

package com.alibaba.hologres.hive;

import com.alibaba.hologres.hive.conf.HoloStorageConfigManager;
import com.alibaba.hologres.hive.input.HoloInputFormat;
import com.alibaba.hologres.hive.output.HoloOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/** BaseHoloStorageHandler. */
public abstract class BaseHoloStorageHandler implements HiveStorageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseHoloStorageHandler.class);
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HoloInputFormat.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HoloOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return HoloSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return null;
    }

    @Override
    public void configureInputJobProperties(
            TableDesc tableDesc, Map<String, String> jobProperties) {
        try {
            LOGGER.debug("Adding properties to input job conf");
            Properties properties = tableDesc.getProperties();
            HoloStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void configureTableJobProperties(
            TableDesc tableDesc, Map<String, String> jobProperties) {
        try {
            LOGGER.debug("Adding properties to job conf");
            Properties properties = tableDesc.getProperties();
            HoloStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void configureOutputJobProperties(
            TableDesc tableDesc, Map<String, String> jobProperties) {
        try {
            LOGGER.debug("Adding properties to output job conf");
            Properties properties = tableDesc.getProperties();
            HoloStorageConfigManager.copyConfigurationToJob(properties, jobProperties);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() {
        return null;
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {}

    @Override
    public String toString() {
        return "Holo JDBC Handler";
    }
}
