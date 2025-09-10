/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Array辅助类.
 *
 * @param <T>
 */
public class ArrayBuffer<T> {
    private T[] buffer;
    private int position;
    private int size;
    private boolean readable = false;
    private final Class<T[]> arrayClass;

    public ArrayBuffer(int initSize, Class<T[]> arrayClass) {
        this.arrayClass = arrayClass;
        this.buffer = (T[]) Array.newInstance(arrayClass.getComponentType(), initSize);
        this.position = 0;
        this.size = initSize;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public int getSize() {
        return size;
    }

    public int remain() {
        return size - position;
    }

    public T last() {
        if (remain() > 0) {
            return buffer[size - 1];
        }
        return null;
    }

    public T pop() {
        if (remain() > 0) {
            T ret = buffer[position];
            buffer[position] = null;
            position++;
            return ret;
        }
        return null;
    }

    public T peek() {
        if (remain() > 0) {
            return buffer[position];
        }
        return null;
    }

    public void add(T r) {
        if (remain() == 0) {
            buffer =
                    Arrays.copyOf(
                            buffer, Math.max(buffer.length + 1, buffer.length * 3 / 2), arrayClass);
            size = buffer.length;
        }
        buffer[position++] = r;
    }

    public void beginRead() {
        size = position;
        position = 0;
        readable = true;
    }

    public void beginWrite() {
        position = 0;
        size = buffer.length;
        readable = false;
    }

    public boolean isReadable() {
        return readable;
    }
}
