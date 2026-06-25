package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.arrow.AbstractArrowWriter;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.utils.CommonUtil;
import com.alibaba.hologres.client.utils.RateLimiter;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;

/** copy in 写入 stage. */
public class CopyInStageWrapper<RECORD> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyInStageWrapper.class);

    private final HoloConfig config;

    // 文件大小, 超过则创建新的文件, 在内存中攒够足够大小发起一次copy in写入
    private final int fileSizeLimit;
    private final String stageName;
    private final String filePrefix;

    private ConnectionHolder connectionHolder;
    private CopyManager copyManager;

    // 文件相关属性
    private int fileIndex = 0;

    // ArrowWriter
    private AbstractArrowWriter<RECORD> arrowWriter;

    private RateLimiter rateLimiter;

    private volatile boolean closed = false;

    public CopyInStageWrapper(
            HoloConfig config,
            String stageName,
            String filePrefix,
            AbstractArrowWriter<RECORD> arrowWriter)
            throws HoloClientException {
        this(config, stageName, filePrefix, arrowWriter, 64 * 1204 * 1024);
    }

    public CopyInStageWrapper(
            HoloConfig config,
            String stageName,
            String filePrefix,
            AbstractArrowWriter<RECORD> arrowWriter,
            int fileSizeLimit)
            throws HoloClientException {
        this.config = config;
        this.fileSizeLimit = fileSizeLimit;
        this.stageName = stageName;
        this.filePrefix = filePrefix;
        this.arrowWriter = arrowWriter;

        initConn();
    }

    /**
     * 初始化字段.
     *
     * @throws HoloClientException 如果初始化失败
     */
    private void initConn() throws HoloClientException {
        boolean isShadingEnv = CommonUtil.detectShadingEnvironment();
        connectionHolder = new ConnectionHolder(config, this, isShadingEnv, true);
    }

    public void putRecord(RECORD record) throws IOException {
        // Acquire rate limit token before processing
        if (rateLimiter != null) {
            try {
                rateLimiter.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("InterruptedException: " + e.getMessage(), e);
            }
        }
        // 将记录放入ArrowWriter中
        arrowWriter.put(record);

        // 检查文件大小是否超过阈值
        if (arrowWriter.getArrowDataSize() >= fileSizeLimit) {
            // 获取数据并清空baos
            byte[] data = arrowWriter.endAndGetBytes();
            copyInStageFromBytes(data);
        }
    }

    public void copyInStageFromBytes(byte[] data) throws IOException {
        if (data.length == 0) {
            return;
        }
        try (ByteArrayInputStream input = new ByteArrayInputStream(data)) {
            String fileName = String.format("%s_%d.arrow", filePrefix, fileIndex++);
            String sql = CopyUtil.buildCopyInStageSql(stageName, fileName);
            connectionHolder.retryExecute(
                    conn -> {
                        try {
                            copyManager = new CopyManager(conn.unwrap(PgConnection.class));
                            LOGGER.info("copy in stage sql({}) begin", sql);
                            long bytes = copyManager.copyIn(sql, input);
                            LOGGER.info(
                                    "copy in stage sql({}) finished, write bytes: {}", sql, bytes);
                        } catch (IOException e) {
                            LOGGER.error("copy in stage sql({}) failed", sql, e);
                            throw new SQLException(e);
                        }

                        return null;
                    },
                    config.getRetryCount());
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
    }

    public void flush() throws IOException {
        byte[] data = arrowWriter.endAndGetBytes();
        copyInStageFromBytes(data);
    }

    public void abort() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        LOGGER.info("CopyInStageWrapper abort begin");
        try {
            arrowWriter.close();
        } finally {
            if (connectionHolder != null) {
                connectionHolder.close();
            }
        }
        LOGGER.info("CopyInStageWrapper abort end");
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        LOGGER.info("CopyInStageWrapper close begin");
        try {
            flush();
        } finally {
            try {
                arrowWriter.close();
            } finally {
                if (connectionHolder != null) {
                    connectionHolder.close();
                }
            }
        }
        LOGGER.info("CopyInStageWrapper close end");
    }

    /**
     * Set the rate limiter for controlling write throughput.
     *
     * @param rateLimiter the rate limiter to use, or null to disable rate limiting
     */
    public void setRateLimiter(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }
}
