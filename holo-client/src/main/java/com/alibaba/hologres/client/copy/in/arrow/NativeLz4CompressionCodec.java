package com.alibaba.hologres.client.copy.in.arrow;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 基于 lz4-java (JNI native) 的 LZ4_FRAME 压缩编解码器.
 *
 * <p>替代 Arrow 默认的 CommonsCompressionFactory (使用 commons-compress 纯 Java 实现), 性能提升约 5-10 倍。lz4-java
 * 底层通过 JNI 调用原生 C LZ4 库。
 *
 * <p>输出格式与 Arrow IPC V5 buffer-level LZ4_FRAME 完全兼容: 每个 buffer 压缩后前缀 8 字节 uncompressed_size
 * (little-endian int64), 后跟 LZ4 Frame 格式压缩数据。
 */
public class NativeLz4CompressionCodec extends AbstractCompressionCodec {

    /**
     * Override compress() to handle empty buffers correctly. AbstractCompressionCodec.compress()
     * writes prefix=0 for empty buffers, but C++ Arrow and pyarrow don't recognize this format. We
     * write prefix=-1 instead (sentinel for "uncompressed"), which all readers support.
     */
    @Override
    public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuf) {
        if (uncompressedBuf.writerIndex() == 0) {
            // Empty buffer: write -1 sentinel (8 bytes) to indicate "not compressed / empty"
            // This is recognized by all Arrow readers (C++, Java, Python)
            ArrowBuf result = allocator.buffer(8);
            result.setLong(0, -1L);
            result.writerIndex(8);
            uncompressedBuf.close();
            return result;
        }
        return super.compress(allocator, uncompressedBuf);
    }

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuf) {
        long uncompressedLen = uncompressedBuf.writerIndex();
        if (uncompressedLen == 0) {
            return uncompressedBuf;
        }

        // 从 ArrowBuf 取出原始字节
        byte[] input = new byte[(int) uncompressedLen];
        uncompressedBuf.getBytes(0, input);

        // 使用 lz4-java native LZ4 Frame 压缩
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) (uncompressedLen / 2));
            try (LZ4FrameOutputStream lz4Out =
                    new LZ4FrameOutputStream(baos, LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB)) {
                lz4Out.write(input);
            }
            byte[] compressed = baos.toByteArray();

            long prefixSize = 8;
            ArrowBuf compressedBuf = allocator.buffer(prefixSize + compressed.length);
            compressedBuf.setBytes(prefixSize, compressed);
            compressedBuf.writerIndex(prefixSize + compressed.length);
            return compressedBuf;
        } catch (IOException e) {
            throw new RuntimeException("LZ4 native compression failed", e);
        }
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuf) {
        long totalLen = compressedBuf.writerIndex();
        if (totalLen == 0) {
            return compressedBuf;
        }

        // AbstractCompressionCodec.decompress() 传入的 buffer 包含 8 字节 uncompressed_size 前缀,
        // doDecompress() 需要跳过这 8 字节, 只取后面的 LZ4 Frame 压缩数据.
        long prefixSize = 8;
        int compressedLen = (int) (totalLen - prefixSize);
        byte[] compressed = new byte[compressedLen];
        compressedBuf.getBytes(prefixSize, compressed);

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) (compressedLen * 4));
            try (LZ4FrameInputStream lz4In =
                    new LZ4FrameInputStream(new ByteArrayInputStream(compressed))) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = lz4In.read(buf)) != -1) {
                    baos.write(buf, 0, n);
                }
            }
            byte[] decompressed = baos.toByteArray();

            ArrowBuf decompressedBuf = allocator.buffer(decompressed.length);
            decompressedBuf.setBytes(0, decompressed);
            decompressedBuf.writerIndex(decompressed.length);
            return decompressedBuf;
        } catch (IOException e) {
            throw new RuntimeException("LZ4 native decompression failed", e);
        }
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.LZ4_FRAME;
    }

    /**
     * NativeLz4CompressionCodec 的 Factory 实现.
     *
     * <p>直接替换 CommonsCompressionFactory.INSTANCE 使用:
     *
     * <pre>
     *   new ArrowStreamWriter(root, null, channel, new IpcOption(),
     *       NativeLz4CompressionCodec.Factory.INSTANCE, CompressionUtil.CodecType.LZ4_FRAME);
     * </pre>
     */
    public static class Factory implements CompressionCodec.Factory {
        public static final Factory INSTANCE = new Factory();

        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
            if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
                return new NativeLz4CompressionCodec();
            }
            throw new IllegalArgumentException("Unsupported codec: " + codecType);
        }

        @Override
        public CompressionCodec createCodec(
                CompressionUtil.CodecType codecType, int compressionLevel) {
            return createCodec(codecType);
        }
    }
}
