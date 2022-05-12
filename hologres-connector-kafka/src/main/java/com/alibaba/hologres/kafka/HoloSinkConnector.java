package com.alibaba.hologres.kafka;

import com.alibaba.hologres.kafka.conf.HoloSinkConfig;
import com.alibaba.hologres.kafka.sink.HoloSinkTask;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** HoloSinkConnector. */
public class HoloSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(HoloSinkConnector.class);
    private static final String VERSION = "hologres-connector-kafka-1.1-SNAPSHOT";

    private Map<String, String> configProps;

    @Override
    public Class<? extends Task> taskClass() {
        return HoloSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        return Collections.nCopies(maxTasks, configProps);
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public void stop() {}

    @Override
    public ConfigDef config() {
        return HoloSinkConfig.config();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }

    @Override
    public String version() {
        return VERSION;
    }
}
