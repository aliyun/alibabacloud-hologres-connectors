package com.alibaba.hologres.hive.conf;

/** HoloStorageConfig. */
public enum HoloStorageConfig {
    TABLE("table", true),
    USERNAME("username", true),
    PASSWORD("password", true),
    JDBC_URL("jdbc.url", true),

    COPY_WRITE_MODE("copy_write_mode", false),
    COPY_WRITE_FORMAT("copy_write_format", false),
    BULK_LOAD("bulk_load", false),
    DIRTY_DATA_CHECK("dirty_data_check", false),
    DIRECT_CONNECT("direct_connect", false),
    MAX_WRITER_NUMBER("max_writer_number", false),
    MAX_WRITER_NUMBER_PER_TASK("max_writer_number_per_task", false),
    MAX_CELL_BUFFER_SIZE("max_cell_buffer_size", false),
    WRITE_MODE("write_mode", false),
    WRITE_BATCH_SIZE("write_batch_size", false),
    WRITE_BATCH_BYTE_SIZE("write_batch_byte_size", false),
    USE_LEGACY_PUT_HANDLER("use_legacy_put_handler", false),
    WRITE_MAX_INTERVAL_MS("write_max_interval_ms", false),
    WRITE_FAIL_STRATEGY("write_fail_strategy", false),
    WRITE_THREAD_SIZE("write_thread_size", false),
    DYNAMIC_PARTITION("dynamic_partition", false),

    READ_THREAD_SIZE("read_thread_size", false),
    READ_BATCH_SIZE("read_batch_size", false),
    READ_BATCH_QUEUE_SIZE("read_batch_queue_size", false),
    SCAN_FETCH_SIZE("scan_fetch_size", false),
    SCAN_TIMEOUT_SECONDS("scan_timeout_seconds", false),
    SCAN_SPLITS("scan_splits", false),
    @Deprecated
    COPY_MODE("copy_mode", false),
    COPY_SCAN_MODE("copy_scan_mode", false),

    RETRY_COUNT("retry_count", false),
    RETRY_SLEEP_INIT_MS("retry_sleep_init_ms", false),
    RETRY_SLEEP_STEP_MS("retry_sleep_step_ms", false),
    CONNECTION_MAX_IDLE_MS("connection_max_idle_ms", false),
    FIXED_CONNECTION_MODE("fixed_connection_mode", false);

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
