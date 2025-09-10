package com.alibaba.hologres.connector.flink.config;

import com.alibaba.hologres.client.copy.CopyMode;

/** WriteMode for sink. */
public enum WriteMode {
    AUTO,
    INSERT,
    COPY_STREAM,
    COPY_BULK_LOAD,
    COPY_BULK_LOAD_ON_CONFLICT;

    public CopyMode toCopyMode() {
        switch (this) {
            case COPY_STREAM:
                return CopyMode.STREAM;
            case COPY_BULK_LOAD:
                return CopyMode.BULK_LOAD;
            case COPY_BULK_LOAD_ON_CONFLICT:
                return CopyMode.BULK_LOAD_ON_CONFLICT;
            case AUTO:
            case INSERT:
            default:
                throw new IllegalArgumentException("WriteMode " + this + " is not a CopyMode");
        }
    }
}
