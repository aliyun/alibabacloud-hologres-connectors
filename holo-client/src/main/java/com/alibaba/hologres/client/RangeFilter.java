/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

/** RangeFilter. */
public class RangeFilter implements Filter {
    int index;
    Object start;
    Object stop;

    public RangeFilter(int index, Object start, Object stop) {
        this.index = index;
        this.start = start;
        this.stop = stop;
    }

    public int getIndex() {
        return index;
    }

    public Object getStart() {
        return start;
    }

    public Object getStop() {
        return stop;
    }
}
