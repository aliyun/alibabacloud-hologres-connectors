package com.alibaba.hologres.kafka.conf;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.utils.ConfLoader;
import com.alibaba.hologres.kafka.exception.KafkaHoloException;
import com.alibaba.hologres.kafka.model.DirtyDataStrategy;
import com.alibaba.hologres.kafka.model.InputFormat;
import com.alibaba.hologres.kafka.utils.DirtyDataUtils;
import com.alibaba.hologres.kafka.utils.JDBCUtils;
import com.alibaba.hologres.kafka.utils.MessageInfo;

import java.util.Map;

/** HoloSinkConfigManager. */
public class HoloSinkConfigManager {
    private final HoloConfig holoConfig;
    private final boolean copyWriteMode;
    private final boolean copyWriteDirtyDataCheck;
    private final String copyWriteFormat;
    private boolean copyWriteDirectConnect;
    private final String tableName;
    private final InputFormat inputFormat;
    private final long initialTimestamp;
    private final DirtyDataUtils dirtyDataUtils = new DirtyDataUtils(DirtyDataStrategy.EXCEPTION);
    private final boolean schemaForceCheck;
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
                config.getString(HoloSinkConfig.DIRTY_DATA_STRATEGY).toLowerCase();
        switch (dirtyDataStrategyStr) {
            case "exception":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.EXCEPTION);
                break;
            case "skip_once":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.SKIP_ONCE);
                dirtyDataUtils.setDirtyDataToSkipOnce(
                        config.getList(HoloSinkConfig.DIRTY_DATA_TO_SKIP_ONCE));
                break;
            case "skip":
                dirtyDataUtils.setDirtyDataStrategy(DirtyDataStrategy.SKIP);
                break;
            default:
                throw new IllegalArgumentException(
                        "Could not recognize DirtyDataStrategy " + dirtyDataStrategyStr);
        }
        schemaForceCheck = config.getBoolean(HoloSinkConfig.SCHEMA_FORCE_CHECK);

        // metrics report interval
        metricsReportInterval = config.getInt(HoloSinkConfig.METRICS_REPORT_INTERVAL);

        // copy write
        copyWriteMode = config.getBoolean(HoloSinkConfig.COPY_WRITE_MODE);
        copyWriteDirtyDataCheck = config.getBoolean(HoloSinkConfig.COPY_WRITE_DIRTY_DATA_CHECK);
        copyWriteFormat = config.getString(HoloSinkConfig.COPY_WRITE_FORMAT);
        copyWriteDirectConnect = config.getBoolean(HoloSinkConfig.COPY_WRITE_DIRECT_CONNECT);

        // holo config
        try {
            holoConfig = new HoloConfig();
            ConfLoader.load(props, "connection.", holoConfig);
            // use jdbc:hologres instead of jdbc:postgresql
            holoConfig.setJdbcUrl(JDBCUtils.formatUrlWithHologres(holoConfig.getJdbcUrl()));
            holoConfig.setInputNumberAsEpochMsForDatetimeColumn(true);
            holoConfig.setUseAKv4(config.getBoolean(HoloSinkConfig.CONNECTION_ENABLE_AKV4));
            if (holoConfig.isUseAKv4()
                    && config.getString(HoloSinkConfig.CONNECTION_AKV4_REGION) != null
                    && !config.getString(HoloSinkConfig.CONNECTION_AKV4_REGION).isEmpty()) {
                holoConfig.setRegion(config.getString(HoloSinkConfig.CONNECTION_AKV4_REGION));
            }
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

    public boolean isSchemaForceCheck() {
        return schemaForceCheck;
    }

    public int getMetricsReportInterval() {
        return metricsReportInterval;
    }

    public HoloConfig getHoloConfig() {
        return holoConfig;
    }

    public boolean isCopyWriteMode() {
        return copyWriteMode;
    }

    public boolean isCopyWriteDirtyDataCheck() {
        return copyWriteDirtyDataCheck;
    }

    public String getCopyWriteFormat() {
        return copyWriteFormat;
    }

    public boolean isCopyWriteDirectConnect() {
        return copyWriteDirectConnect;
    }

    public void setCopyWriteDirectConnect(boolean copyWriteDirectConnect) {
        this.copyWriteDirectConnect = copyWriteDirectConnect;
    }

    public HoloClient getClient() {
        try {
            HoloClient client = new HoloClient(holoConfig);
            client.setAsyncCommit(true);
            return client;
        } catch (Exception e) {
            throw new KafkaHoloException(e);
        }
    }

    public MessageInfo getMessageInfo() {
        return messageInfo;
    }
}
