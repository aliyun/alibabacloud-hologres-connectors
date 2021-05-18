package com.alibaba.hologres.hive.conf;

/** HoloStorageConfig. */
public enum HoloStorageConfig {
    TABLE("table", true),
    USERNAME("username", false),
    PASSWORD("password", false),

    JDBC_URL("jdbc.url", true),
    WRITE_MODE("write_mode", false),
    WRITE_BATCH_SIZE("write_batch_size", false),
    WRITE_BATCH_BYTE_SIZE("write_batch_byte_size", false),
    WRITE_MAX_INTERVAL_MS("write_max_interval_ms", false),
    WRITE_FAIL_STRATEGY("write_fail_strategy", false),
    WRITE_THREAD_SIZE("write_thread_size", false),
    RETRY_COUNT("retry_count", false),
    RETRY_SLEEP_INIT_MS("retry_sleep_init_ms", false),
    RETRY_SLEEP_STEP_MS("retry_sleep_step_ms", false),
    CONNECTION_MAX_IDLE_MS("connection_max_idle_ms", false);

    private String propertyName;
    private boolean required = false;

    HoloStorageConfig(String propertyName, boolean required) {
        this.propertyName = propertyName;
        this.required = required;
    }

    HoloStorageConfig(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return "hive.sql." + this.propertyName;
    }

    public boolean isRequired() {
        return this.required;
    }
}
