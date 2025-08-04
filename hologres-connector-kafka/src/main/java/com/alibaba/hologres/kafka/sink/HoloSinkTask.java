package com.alibaba.hologres.kafka.sink;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.kafka.conf.HoloSinkConfigManager;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
import com.alibaba.hologres.kafka.utils.JDBCUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** HoloSinkTask. */
public class HoloSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(HoloSinkTask.class);
    private static final String classname = HoloSinkTask.class.getName();
    HoloSinkWriter holoSinkWriter;

    @Override
    public void start(Map<String, String> props) {
        logger.info(
                "[{}] Entry {}.start, props={}", Thread.currentThread().getId(), classname, props);

        try {
            HoloSinkConfigManager configManager = new HoloSinkConfigManager(props);

            String tableName = configManager.getTableName();
            try (HoloClient client = configManager.getClient()) {
                TableSchema tableSchema = client.getTableSchema(tableName);
                HoloVersion holoVersion;
                try {
                    holoVersion = client.sql(ConnectionUtil::getHoloVersion).get();
                } catch (Exception e) {
                    throw new KafkaHoloException("Failed to get holo version", e);
                }
                boolean supportCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0;
                if (!supportCopy) {
                    logger.warn(
                            "The hologres instance version is {}, but only instances greater than 1.3.24 support copy "
                                    + "write mode",
                            holoVersion);
                }

                if (configManager.isCopyWriteDirectConnect()) {
                    configManager.setCopyWriteDirectConnect(
                            JDBCUtils.couldDirectConnect(configManager));
                }

                if (configManager.getMetricsReportInterval() != -1) {
                    Metrics.startSlf4jReporter(
                            configManager.getMetricsReportInterval(), TimeUnit.SECONDS);
                }
                holoSinkWriter =
                        new HoloSinkWriter(
                                configManager,
                                tableSchema,
                                configManager.isCopyWriteMode() && supportCopy);
            }
        } catch (KafkaHoloException | HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the hologres...",
                recordsCount,
                first.topic(),
                first.kafkaPartition(),
                first.kafkaOffset());
        try {
            holoSinkWriter.write(records);
        } catch (KafkaHoloException | HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        super.flush(currentOffsets);
        holoSinkWriter.flush();
    }

    @Override
    public void stop() {
        if (holoSinkWriter != null) {
            holoSinkWriter.close();
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
