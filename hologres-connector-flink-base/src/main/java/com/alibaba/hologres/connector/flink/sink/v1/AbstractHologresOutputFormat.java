/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.connector.flink.sink.v1;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.connector.flink.api.HologresWriter;
import com.alibaba.hologres.connector.flink.config.DirtyDataStrategy;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Hologres sync output format implement. */
public abstract class AbstractHologresOutputFormat<T> extends RichOutputFormat<T>
        implements Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHologresOutputFormat.class);
    private static final long serialVersionUID = 5113221824076190115L;

    protected final HologresConnectionParam param;
    protected boolean ignoreDelete;

    protected Counter outTps;
    protected Counter outBps;
    protected Counter sinkSkipCounter;

    protected HologresWriter<T> hologresIOClient;

    protected DirtyDataStrategy dirtyDataStrategy;
    protected Tuple2<String, Exception> exception = null;

    public AbstractHologresOutputFormat(
            HologresConnectionParam param, HologresWriter<T> hologresIOClient) {
        this.param = checkNotNull(param);

        // set params
        this.ignoreDelete = param.isIgnoreDelete();
        this.hologresIOClient = hologresIOClient;
        this.dirtyDataStrategy = param.getDirtyDataStrategy();
    }

    @Override
    public void open(InitializationContext context) throws IOException {
        exception = null;
        LOG.info(
                "Opening {} for frontend: {}, database: {} and table: {}",
                getClass().getSimpleName(),
                param.getEndpoint(),
                param.getDatabase(),
                param.getTable());
        RuntimeContext runtimeContext = getRuntimeContext();

        outTps = runtimeContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        outBps = runtimeContext.getMetricGroup().getIOMetricGroup().getNumBytesOutCounter();
        sinkSkipCounter =
                runtimeContext
                        .getMetricGroup()
                        .getIOMetricGroup()
                        .counter("sinkSkipDirtyDataCounter");
        hologresIOClient.open(context.getTaskNumber(), context.getNumTasks());
        LOG.info("Finished opening {}", getClass().getSimpleName());
    }

    @Override
    public void close() throws IOException {
        LOG.info("start to closing {} {}", param.getTable(), getClass().getSimpleName());
        // flush requests
        flush();
        if (hologresIOClient != null) {
            hologresIOClient.close();
        }
        LOG.info("Finished closing {} {}", param.getTable(), getClass().getSimpleName());
    }

    @Override
    public void writeRecord(T value) throws IOException {
        if (Objects.nonNull(exception)) {
            throw new IOException(
                    String.format(
                            "An exception occurred in the sync operation called during the last checkpoint. at %s.",
                            exception.f0),
                    exception.f1);
        }
        if (outTps != null) {
            outTps.inc();
        }
        try {
            long writeBytes = writeData(value);
            if (outBps != null && writeBytes > 0) {
                outBps.inc(writeBytes);
            }
        } catch (HoloClientException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    value,
                    ExceptionUtils.getStackTrace(e));
            // Only a specific ERROR_CODE indicates dirty data, so dirtyDataStrategy can choose to
            // ignore the exception. Other exceptions cannot be skipped.
            if (dirtyDataStrategy.equals(DirtyDataStrategy.SKIP)
                    && (e.getCode() == ExceptionCode.DATA_VALUE_ERROR
                            || e.getCode() == ExceptionCode.DATA_TYPE_ERROR)) {
                sinkSkipCounter.inc();
            } else {
                throw new IOException(e);
            }
        } catch (IOException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    value,
                    ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    public abstract long writeData(T data) throws HoloClientException, IOException;

    @Override
    public void flush() throws IOException {
        LOG.info("{} start to wait request to finish", param.getTable());
        try {
            hologresIOClient.flush();
            LOG.info("{} end to wait request to finish", param.getTable());
        } catch (HoloClientException e) {
            LOG.error("Flush messages failed", e);
            // Only a specific ERROR_CODE indicates dirty data, so dirtyDataStrategy can choose to
            // ignore the exception. Other exceptions cannot be skipped.
            if (dirtyDataStrategy.equals(DirtyDataStrategy.SKIP)
                    && (e.getCode() == ExceptionCode.DATA_VALUE_ERROR
                            || e.getCode() == ExceptionCode.DATA_TYPE_ERROR)) {
                sinkSkipCounter.inc();
            } else {
                exception = new Tuple2<>(LocalDateTime.now().toString(), e);
                throw new IOException(e);
            }
        } catch (IOException e) {
            // When checkpointing, this sync will be called(see OutputFormatSinkFunction
            // snapshotState).
            // If this sync(flush) throws an exception due to dirty data or other reasons, the
            // checkpoint will fail. However, checkpoint has a failure-ignoring configuration
            // "execution.checkpointing.tolerable-failed-checkpoints".
            // If this failure is ignored, this batch of data may be lost. so if checkpoint
            // failed because write
            // error, we throw exception later, make sure the job will fail over successfully.
            LOG.error("Flush messages failed", e);
            exception = new Tuple2<>(LocalDateTime.now().toString(), e);
            throw e;
        }
    }

    @Override
    public void configure(Configuration configuration) {}
}
