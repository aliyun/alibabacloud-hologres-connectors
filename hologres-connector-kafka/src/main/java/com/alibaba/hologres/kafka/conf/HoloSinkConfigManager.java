package com.alibaba.hologres.kafka.conf;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
import com.alibaba.hologres.kafka.model.DirtyDataStrategy;
import com.alibaba.hologres.kafka.model.InputFormat;
import com.alibaba.hologres.kafka.utils.DirtyDataUtils;
import com.alibaba.hologres.kafka.utils.MessageInfo;

import java.util.Map;

/** HoloSinkConfigManager. */
public class HoloSinkConfigManager {
    private final HoloClient client;
    private final String tableName;
    private final InputFormat inputFormat;
    private final long initialTimestamp;
    private final DirtyDataUtils dirtyDataUtils = new DirtyDataUtils(DirtyDataStrategy.EXCEPTION);
    private final MessageInfo messageInfo = new MessageInfo(true);
    private final int metricsReportInterval;

    public HoloSinkConfigManager(Map<String, String> props) throws KafkaHoloException {
        HoloSinkConfig config = new HoloSinkConfig(props);

        tableName = config.getString(HoloSinkConfig.TABLE);
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name should be defined");
        }

        // Input Format
        String inputFormatStr = config.getString(HoloSinkConfig.INPUT_FORMAT).toLowerCase();
        switch (inputFormatStr) {
            case "string":
                inputFormat = InputFormat.STRING;
                break;
            case "struct_json":
                inputFormat = InputFormat.STRUCT_JSON;
                break;
            case "json":
                inputFormat = InputFormat.JSON;
                break;
            default:
                throw new IllegalArgumentException(
                        "Could not recognize InputFormat " + inputFormatStr);
        }

        // message information
        messageInfo.wholeMessageInfo = config.getBoolean(HoloSinkConfig.WHOLE_MESSAGE_INFO);
        if (messageInfo.wholeMessageInfo) {
            messageInfo.setMessageInfo(
                    config.getString(HoloSinkConfig.MESSAGE_TOPIC).toLowerCase(),
                    config.getString(HoloSinkConfig.MESSAGE_PARTITION).toLowerCase(),
                    config.getString(HoloSinkConfig.MESSAGE_OFFSET).toLowerCase(),
                    config.getString(HoloSinkConfig.MESSAGE_TIMESTAMP).toLowerCase());
        }

        // initial timestamp
        initialTimestamp = config.getLong(HoloSinkConfig.INITIAL_TIMESTAMP);

        // dirty data strategy
        String dirtyDataStrategyStr =
                config.getString(HoloSinkConfig.DIRTY_DATE_STRATEGY).toLowerCase();
        switch (dirtyDataStrategyStr) {
            case "exception":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.EXCEPTION);
                break;
            case "skip_once":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.SKIP_ONCE);
                dirtyDataUtils.setDirtyDataToSkipOnce(
                        config.getList(HoloSinkConfig.DIRTY_DATE_TO_SKIP_ONCE));
                break;
            case "skip":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.SKIP);
                break;
            default:
                throw new IllegalArgumentException(
                        "Could not recognize DirtyDataStrategy " + dirtyDataStrategyStr);
        }

        // metrics report interval
        metricsReportInterval = config.getInt(HoloSinkConfig.METRICS_REPORT_INTERVAL);

        // holo client
        try {
            HoloConfig holoConfig = new HoloConfig();
            ConfLoader.load(props, "connection.", holoConfig);
            holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true);

            client = new HoloClient(holoConfig);
            client.setAsyncCommit(true);
        } catch (Exception e) {
            throw new KafkaHoloException(e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public InputFormat getInputFormat() {
        return inputFormat;
    }

    public long getInitialTimestamp() {
        return initialTimestamp;
    }

    public DirtyDataUtils getDirtyDataUtils() {
        return dirtyDataUtils;
    }

    public int getMetricsReportInterval() {
        return metricsReportInterval;
    }

    public HoloClient getClient() {
        return client;
    }

    public MessageInfo getMessageInfo() {
        return messageInfo;
    }
}
