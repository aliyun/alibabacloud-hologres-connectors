/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy.out;

import com.alibaba.hologres.client.copy.WithCopyResult;
import org.postgresql.copy.CopyOut;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/** 把CopyOut封装成一个InputStream. */
public class CopyOutInputStream extends InputStream implements WithCopyResult {

    private static final int DEFAULT_MAX_CELL_BUFFER_SIZE = 20 * 1024 * 1024;

    private final CopyOut copyOut;
    private ByteBuffer buffer;
    private final int maxBufferSize;

    private long result = 0;

    public CopyOutInputStream(CopyOut copyOut) {
        this(copyOut, DEFAULT_MAX_CELL_BUFFER_SIZE);
    }

    public CopyOutInputStream(CopyOut copyOut, int maxBufferSize) {
        this.copyOut = copyOut;
        this.maxBufferSize = maxBufferSize;
        this.buffer = ByteBuffer.allocate(1024);
        buffer.position(buffer.limit());
    }

    @Override
    public int read(byte[] b) throws IOException {
        // 如果buffer为空，则填充
        if (!buffer.hasRemaining()) {
            fillBuffer();
            if (!buffer.hasRemaining()) {
                return -1;
            }
        }
        // 强制读取完buffer中的所有数据
        if (b.length < buffer.remaining()) {
            throw new IOException("Buffer is too small, need " + buffer.remaining() + " bytes");
        }
        int bytesToCopy = buffer.remaining();
        buffer.get(b, 0, bytesToCopy);
        return bytesToCopy;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!buffer.hasRemaining()) {
            fillBuffer();
            if (!buffer.hasRemaining()) {
                return -1;
            }
        }

        int bytesToCopy = Math.min(len, buffer.remaining());
        buffer.get(b, off, bytesToCopy);
        return bytesToCopy;
    }

    private void fillBuffer() throws IOException {
        // 只有buffer为空时，才填充
        try {
            if (!copyOut.isActive()) {
                // CopyDone
                buffer.limit(0); // 设置为空，标记结束
                return;
            }
            byte[] data = copyOut.readFromCopy();
            if (data == null) {
                buffer.limit(0); // 设置为空，标记结束
            } else {
                mayIncBuffer(data.length);
                buffer.clear();
                buffer.put(data);
                buffer.flip();
            }
        } catch (Exception e) {
            throw new IOException("Error reading from CopyOut", e);
        }
    }

    private void mayIncBuffer(int size) throws IOException {
        if (buffer.remaining() < size) {
            if (buffer.position() + size < maxBufferSize) {
                int target =
                        Math.min(
                                Math.max(buffer.position() + size, buffer.position() * 2),
                                maxBufferSize);
                ByteBuffer temp = ByteBuffer.allocate(target);
                buffer.flip();
                temp.put(buffer);
                buffer.clear();
                buffer = temp;
            } else {
                throw new IOException("CopyOutInputStream buffer exceed max size " + maxBufferSize);
            }
        }
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("please use int read(byte b[])");
    }

    @Override
    public void close() throws IOException {
        result = copyOut.getHandledRowCount();
        try {
            if (copyOut.isActive()) {
                copyOut.cancelCopy();
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long getResult() {
        return result;
    }
}
