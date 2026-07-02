package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.copy.CopyUtil;
import com.alibaba.hologres.client.copy.in.arrow.AbstractArrowWriter;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.utils.CommonUtil;
import com.alibaba.hologres.client.utils.RateLimiter;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** copy in 写入 stage. */
public class CopyInStageWrapper<RECORD> implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyInStageWrapper.class);
    /** 每条 CopyData 协议消息的最大字节数. */
    private static final int COPY_DATA_CHUNK_SIZE = 128 * 1024;

    /** 支持 Arrow IPC V5 buffer-level LZ4 压缩的最小 Hologres 实例版本. */
    private static final HoloVersion MIN_VERSION_FOR_COMPRESSION = new HoloVersion(4, 2, 7);

    private final HoloConfig config;
    // 文件大小, 超过则创建新的文件, 在内存中攒够足够大小发起一次copy in写入
    private final int fileSizeLimit;
    private final String stageName;
    /** 文件前缀，子类构造文件名时可见. */
    protected final String filePrefix;

    private volatile boolean closed = false;
    private ConnectionHolder connectionHolder;
    private CopyManager copyManager;
    // 文件相关属性
    /** 文件序号，子类构造文件名时可见. */
    protected int fileIndex = 0;
    // ArrowWriter
    private AbstractArrowWriter<RECORD> arrowWriter;
    private RateLimiter rateLimiter;
    /** 是否已经根据实例版本校验过压缩兼容性. */
    private volatile boolean compressionCapabilityChecked;

    public CopyInStageWrapper(
            HoloConfig config,
            String stageName,
            String filePrefix,
            AbstractArrowWriter<RECORD> arrowWriter) {
        this(config, stageName, filePrefix, arrowWriter, 64 * 1024 * 1024);
    }

    public CopyInStageWrapper(
            HoloConfig config,
            String stageName,
            String filePrefix,
            AbstractArrowWriter<RECORD> arrowWriter,
            int fileSizeLimit) {
        this.config = config;
        this.fileSizeLimit = fileSizeLimit;
        this.stageName = stageName;
        this.filePrefix = filePrefix;
        this.arrowWriter = arrowWriter;
    }

    /**
     * 初始化数据库连接（lazy，首次调用 copyInStageFromBytes 时触发）.
     *
     * @throws HoloClientException 如果初始化失败
     */
    private void initConn() throws HoloClientException {
        boolean isShadingEnv = CommonUtil.detectShadingEnvironment();
        connectionHolder = new ConnectionHolder(config, this, isShadingEnv, true);
    }

    /**
     * 根据 Hologres 实例版本校验压缩兼容性. 如果启用了压缩但实例版本低于 {@link #MIN_VERSION_FOR_COMPRESSION}，
     * 直接抛出异常，避免把不支持的压缩数据发送到后端.
     *
     * @throws IOException 如果实例版本不支持压缩或获取版本失败
     */
    void checkCompressionCapability() throws IOException {
        if (compressionCapabilityChecked) {
            return;
        }
        synchronized (this) {
            if (compressionCapabilityChecked) {
                return;
            }
            try {
                if (!arrowWriter.isCompressionEnabled()) {
                    return;
                }
                if (connectionHolder == null) {
                    initConn();
                }
                HoloVersion version = connectionHolder.getVersion();
                if (version == null || version.isUndefined()) {
                    throw new IOException(
                            "Unable to determine Hologres instance version, "
                                    + "cannot verify whether Arrow IPC LZ4 compression is supported");
                }
                if (version.compareTo(MIN_VERSION_FOR_COMPRESSION) < 0) {
                    throw new IOException(
                            String.format(
                                    "Hologres instance version %s does not support Arrow IPC LZ4 compression, "
                                            + "minimum required version is %s. "
                                            + "Please disable compression or upgrade the instance.",
                                    version.toVersionString(),
                                    MIN_VERSION_FOR_COMPRESSION.toVersionString()));
                }
            } catch (HoloClientException e) {
                throw new IOException(
                        "Failed to check compression capability by instance version", e);
            } finally {
                compressionCapabilityChecked = true;
            }
        }
    }

    public void putRecord(RECORD record) throws IOException {
        checkCompressionCapability();

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
        if (connectionHolder == null) {
            try {
                initConn();
            } catch (HoloClientException e) {
                throw new IOException("Failed to initialize connection to stage", e);
            }
        }
        String fileName = String.format("%s_%d.arrow", filePrefix, fileIndex++);
        String sql = CopyUtil.buildCopyInStageSql(stageName, fileName);
        try {
            connectionHolder.retryExecute(
                    conn -> {
                        copyManager = new CopyManager(conn.unwrap(PgConnection.class));
                        LOGGER.info("copy in stage sql({}) begin", sql);
                        CopyIn cp = copyManager.copyIn(sql);
                        try {
                            for (int off = 0; off < data.length; off += COPY_DATA_CHUNK_SIZE) {
                                int len = Math.min(COPY_DATA_CHUNK_SIZE, data.length - off);
                                cp.writeToCopy(data, off, len);
                            }
                            long bytes = cp.endCopy();
                            LOGGER.info(
                                    "copy in stage sql({}) finished, write bytes: {}", sql, bytes);
                        } finally {
                            if (cp.isActive()) {
                                cp.cancelCopy();
                            }
                        }
                        return null;
                    },
                    config.getRetryCount());
        } catch (HoloClientException e) {
            throw new IOException(e);
        }
    }

    public void flush() throws IOException {
        checkCompressionCapability();
        byte[] data = arrowWriter.endAndGetBytes();
        copyInStageFromBytes(data);
    }

    /** 异常路径下放弃未提交数据，仅释放资源（不 flush）. */
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

    public int getFileIndex() {
        return fileIndex;
    }
}
