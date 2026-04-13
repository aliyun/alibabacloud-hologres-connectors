package com.alibaba.hologres.client.copy.out.arrow;

import com.alibaba.hologres.client.copy.WithCopyResult;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** 从CopyOutInputStream中以批的形式读取数据. 每一批数据是一个VectorSchemaRoot */
public class ArrowReader implements Closeable {
    public static class UncompressedOnlyFactory implements CompressionCodec.Factory {
        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
            return NoCompressionCodec.INSTANCE;
        }

        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int i) {
            return NoCompressionCodec.INSTANCE;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowReader.class);

    private final InputStream is;
    private final RootAllocator allocator;
    private boolean isCompressed = false;
    private boolean headerHandled = false;
    private ArrowStreamReader arrowReader;
    private CompressionCodec.Factory compressFactory;
    private VectorSchemaRoot currentBatch;

    private long totalCompressedSize = 0;
    private long totalDecompressedSize = 0;

    public ArrowReader(InputStream is) {
        this.is = is;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.compressFactory = new UncompressedOnlyFactory();
    }

    public ArrowReader(InputStream is, boolean isCompressed) {
        this(is);
        this.isCompressed = isCompressed;
    }

    boolean closed = false;

    public long getResult() {
        if (is instanceof WithCopyResult) {
            return ((WithCopyResult) is).getResult();
        } else {
            return -1;
        }
    }

    @Override
    public void close() throws IOException {
        if (isCompressed) {
            LOGGER.info(
                    "read compressed data, total compressed data size: {}, total decompressed data size: {}",
                    totalCompressedSize,
                    totalDecompressedSize);
        }
        if (!closed) {
            closed = true;
            try {
                if (arrowReader != null) {
                    arrowReader.close();
                    arrowReader = null;
                    currentBatch = null;
                }
            } finally {
                try {
                    is.close();
                } finally {
                    allocator.close();
                }
            }
        }
    }

    /**
     * 获取当前批次的数据,在调用nextBatch确认有数据时调用.
     *
     * @return 当前批次的数据
     */
    public VectorSchemaRoot getCurrentValue() {
        return currentBatch;
    }

    /**
     * 尝试读取下一个批次的数据, 如果有数据会填充到currentBatch并返回ture.
     *
     * @return 是否有下一个批次的数据
     * @throws IOException 如果读取失败
     */
    public boolean nextBatch() throws IOException {
        if (!headerHandled) {
            handleHeader();
        }

        int numOfFields = readNumOfFields();
        if (numOfFields == -1) {
            // 表示读取到数据的结尾
            return false;
        } else if (numOfFields != 1) {
            // 读取copy arrow的结果,这个字段值只能是1
            throw new IOException("Expect only one field.");
        }

        int arrowDataLength = readArrowDataLength();
        if (arrowDataLength > 0) {
            byte[] arrowData = readArrowData(arrowDataLength);
            if (isCompressed) {
                arrowData = decompress(arrowData);
            }
            if (arrowReader != null) {
                arrowReader.close();
                arrowReader = null;
            }
            arrowReader =
                    new ArrowStreamReader(
                            new ByteArrayInputStream(arrowData), allocator, compressFactory);
        } else {
            return false;
        }

        boolean hasNext = arrowReader.loadNextBatch();
        if (hasNext) {
            currentBatch = arrowReader.getVectorSchemaRoot();
        } else {
            currentBatch = null;
        }
        return hasNext;
    }

    /**
     * 处理头部信息.
     *
     * <p>11 byte: PGCOPY\n\377\r\n\0
     *
     * <p>4 byte: Flags field
     *
     * <p>4 byte: headers remain (剩下的header，不包括自己)
     *
     * @throws IOException 如果读取失败
     */
    private void handleHeader() throws IOException {
        byte[] headerBytes = new byte[19];
        int size = is.read(headerBytes, 0, 19);
        if (size != 19) {
            throw new IOException("Invalid header.");
        }
        headerHandled = true;
    }

    /**
     * 读取字段数量.
     *
     * <p>2 byte: 字段数量
     *
     * @return 字段数量
     * @throws IOException 如果读取失败
     */
    private int readNumOfFields() throws IOException {
        byte[] numOfFieldBytes = new byte[2];
        int size = is.read(numOfFieldBytes, 0, 2);
        if (size == -1) {
            return -1;
        }
        if (size != 2) {
            throw new IOException("Invalid numOfFields.");
        }
        return ByteBuffer.wrap(numOfFieldBytes, 0, 2).order(ByteOrder.BIG_ENDIAN).getShort();
    }

    /**
     * 读取Arrow数据长度.
     *
     * <p>4 byte: 本批Arrow数据总长度
     *
     * @return Arrow数据长度
     * @throws IOException 如果读取失败
     */
    private int readArrowDataLength() throws IOException {
        byte[] arrowDataLengthBytes = new byte[4];
        int size = is.read(arrowDataLengthBytes, 0, 4);
        if (size != 4) {
            throw new IOException("Invalid arrowDataLength.");
        }
        return ByteBuffer.wrap(arrowDataLengthBytes, 0, 4).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    /**
     * 读取Arrow数据.
     *
     * @param length 数据长度
     * @return Arrow数据
     * @throws IOException 如果读取失败
     */
    private byte[] readArrowData(int length) throws IOException {
        byte[] arrowData = new byte[length];
        int size = is.read(arrowData, 0, length);
        if (size != length) {
            throw new IOException("Invalid arrowData.");
        }
        return arrowData;
    }

    private byte[] decompress(byte[] compressedData) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(compressedData);
        int decompressedSize = buffer.getInt();
        LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();
        ByteBuffer decompressed = ByteBuffer.wrap(new byte[decompressedSize]);
        try {
            decompressor.decompress(
                    buffer,
                    Integer.BYTES,
                    compressedData.length - Integer.BYTES,
                    decompressed,
                    0,
                    decompressedSize);
        } catch (Exception e) {
            throw new IOException(
                    "decompress data failed, may be choose copy without compress.", e);
        }

        totalCompressedSize += compressedData.length;
        totalDecompressedSize += decompressedSize;
        return decompressed.array();
    }
}
