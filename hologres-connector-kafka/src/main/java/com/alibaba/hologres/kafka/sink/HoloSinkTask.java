package com.alibaba.hologres.kafka.sink;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.utils.Metrics;
import com.alibaba.hologres.kafka.conf.HoloSinkConfigManager;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
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
    HoloClient client;
    HoloSinkWriter holoSinkWriter;

    @Override
    public void start(Map<String, String> props) {
        logger.info(
                "[{}] Entry {}.start, props={}", Thread.currentThread().getId(), classname, props);

        try {
            HoloSinkConfigManager configManager = new HoloSinkConfigManager(props);

            String tableName = configManager.getTableName();
            client = configManager.getClient();

            if (configManager.getMetricsReportInterval() != -1) {
                Metrics.startSlf4jReporter(
                        configManager.getMetricsReportInterval(), TimeUnit.SECONDS);
            }

            holoSinkWriter =
                    new HoloSinkWriter(
                            client,
                            tableName,
                            configManager.getMessageInfo(),
                            configManager.getInputFormat(),
                            configManager.getInitialTimestamp(),
                            configManager.getDirtyDataUtils());
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
        try {
            client.flush();
        } catch (HoloClientException e) {
            throw new KafkaHoloException(e);
        }
    }

    @Override
    public void stop() {
        if (client != null) {
            try {
                client.flush();
            } catch (HoloClientException e) {
                throw new KafkaHoloException(e);
            }
            client.close();
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
