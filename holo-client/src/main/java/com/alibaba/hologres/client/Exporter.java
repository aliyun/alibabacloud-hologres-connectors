/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;

import java.io.OutputStream;
import java.security.InvalidParameterException;

/** Exporter. */
public class Exporter {
    private TableSchema schema;
    int startShardId = -1;
    int endShardId = -1;
    OutputStream os;
    int threadSize = 1;

    Exporter(
            TableSchema schema, int startShardId, int endShardId, OutputStream os, int threadSize) {
        this.schema = schema;
        this.startShardId = startShardId;
        this.endShardId = endShardId;
        this.os = os;
        this.threadSize = threadSize;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public int getStartShardId() {
        return startShardId;
    }

    public int getEndShardId() {
        return endShardId;
    }

    public OutputStream getOutputStream() {
        return os;
    }

    public int getThreadSize() {
        return threadSize;
    }

    public static Exporter.Builder newBuilder(TableSchema schema) {
        return new Exporter.Builder(schema);
    }

    /** builder. */
    public static class Builder {
        private TableSchema schema;
        int startShardId = -1;
        int endShardId = -1;
        OutputStream os;
        int threadSize = 1;

        public Builder(TableSchema schema) {
            this.schema = schema;
        }

        public Builder setShardRange(int shardId) {
            setShardRange(shardId, shardId + 1);
            return this;
        }

        /**
         * 输出shardId [start,end) 的数据.
         *
         * @param startShardId start
         * @param endShardId end
         * @return
         */
        public Builder setShardRange(int startShardId, int endShardId) {
            if (endShardId <= startShardId) {
                throw new InvalidParameterException("startShardId must less then endShardId");
            }
            this.startShardId = startShardId;
            this.endShardId = endShardId;
            return this;
        }

        public Builder setOutputStream(OutputStream os) {
            this.os = os;
            this.threadSize = 1;
            return this;
        }

        public Builder setThreadSize(int threadSize) {
            if (this.os == null) {
                this.threadSize = threadSize;
            }
            return this;
        }

        public Exporter build() {
            return new Exporter(schema, startShardId, endShardId, os, threadSize);
        }
    }
}
