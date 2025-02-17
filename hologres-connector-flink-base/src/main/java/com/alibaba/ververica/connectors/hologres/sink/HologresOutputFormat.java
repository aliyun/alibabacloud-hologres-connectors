package com.alibaba.ververica.connectors.hologres.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;

import com.alibaba.ververica.connectors.common.MetricUtils;
import com.alibaba.ververica.connectors.common.sink.HasRetryTimeout;
import com.alibaba.ververica.connectors.common.sink.Syncable;
import com.alibaba.ververica.connectors.hologres.api.HologresWriter;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Hologres sync output format implement. */
public class HologresOutputFormat<T> extends RichOutputFormat<T>
        implements Syncable, HasRetryTimeout {
    private static final Logger LOG = LoggerFactory.getLogger(HologresOutputFormat.class);
    private static final long serialVersionUID = 5113221824076190115L;

    protected final HologresConnectionParam param;
    protected boolean ignoreDelete;

    protected Meter outTps;
    protected Meter outBps;
    protected Counter sinkSkipCounter;
    protected HologresWriter<T> hologresIOClient;

    protected Exception exception = null;

    public HologresOutputFormat(HologresConnectionParam param, HologresWriter<T> hologresIOClient) {
        this.param = checkNotNull(param);

        // set params
        this.ignoreDelete = param.isIgnoreDelete();
        this.hologresIOClient = hologresIOClient;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info(
                "Opening {} for frontend: {}, database: {} and table: {}",
                getClass().getSimpleName(),
                param.getEndpoint(),
                param.getDatabase(),
                param.getTable());
        RuntimeContext runtimeContext = getRuntimeContext();
        outTps = MetricUtils.registerNumRecordsOutRate(runtimeContext);
        outBps = MetricUtils.registerNumBytesOutRate(runtimeContext, "hologres");
        sinkSkipCounter = MetricUtils.registerNumRecordsOutErrors(runtimeContext);
        hologresIOClient.open(
                runtimeContext.getIndexOfThisSubtask(),
                runtimeContext.getNumberOfParallelSubtasks());
        LOG.info("Finished opening {}", getClass().getSimpleName());
    }

    @Override
    public void close() throws IOException {
        // flush requests
        try {
            hologresIOClient.flush();
            hologresIOClient.close();
        } catch (IOException e) {
            throw new IOException(e);
        }
        LOG.info("Finished closing {}", getClass().getSimpleName());
    }

    @Override
    public void writeRecord(T value) throws IOException {
        if (exception != null) {
            throw new IOException(exception);
        }
        if (outTps != null) {
            outTps.markEvent();
        }
        try {
            long writeBytes = hologresIOClient.writeAddRecord(value);
            if (outBps != null && writeBytes > 0) {
                outBps.markEvent(writeBytes);
            }
        } catch (IOException e) {
            LOG.error(
                    "Upsert data '{}' failed, caused by {}",
                    value,
                    ExceptionUtils.getStackTrace(e));
            throw new IOException(e);
        }
    }

    @Override
    public long getRetryTimeout() {
        return 0;
    }

    @Override
    public void sync() throws IOException {
        LOG.info("start to wait request to finish");
        try {
            hologresIOClient.flush();
            LOG.info("end to wait request to finish");
        } catch (IOException e) {
            // When checkpointing, this sync will be called(see OutputFormatSinkFunction
            // snapshotState).
            // If this sync(flush) throws an exception due to dirty data or other reasons, the
            // checkpoint will fail. However, checkpoint has a failure-ignoring configuration
            // "execution.checkpointing.tolerable-failed-checkpoints".
            // If this failure is ignored, this batch of data may be lost. so if checkpoint
            // failed because write
            // error, we throw exception later, make sure the job will fail over successfully.
            exception = e;
            throw new IOException(e);
        }
    }

    @Override
    public void configure(Configuration configuration) {}
}
