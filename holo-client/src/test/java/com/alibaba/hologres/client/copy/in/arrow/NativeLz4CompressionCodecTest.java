package com.alibaba.hologres.client.copy.in.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * {@link NativeLz4CompressionCodec} 单测.
 *
 * <p>核心回归: 空 buffer 压缩后必须写 -1 sentinel (8 字节), 不能写 0 或不写前缀, 否则 C++/pyarrow 解压失败.
 */
public class NativeLz4CompressionCodecTest {

    /** 空 buffer 压缩后应返回 8 字节且内容为 -1 sentinel. */
    @Test
    public void testCompressEmptyBufferReturnsNegativeOneSentinel() {
        NativeLz4CompressionCodec codec = new NativeLz4CompressionCodec();
        try (RootAllocator allocator = new RootAllocator()) {
            ArrowBuf emptyBuf = allocator.buffer(8);
            emptyBuf.writerIndex(0);

            ArrowBuf compressed = codec.compress(allocator, emptyBuf);
            try {
                Assert.assertEquals(compressed.writerIndex(), 8L, "空 buffer 压缩后应为 8 字节前缀");
                Assert.assertEquals(compressed.getLong(0), -1L, "空 buffer 压缩前缀必须为 -1 sentinel");
            } finally {
                compressed.close();
            }
        }
    }
}
