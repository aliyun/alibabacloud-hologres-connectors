/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

/** RangeFilter. */
public class RangeFilter implements Filter {
    int index;
    Object start;
    Object stop;
    boolean isStartInclude = true;
    boolean isStopInclude = false;

    public RangeFilter(
            int index, Object start, Object stop, boolean isStartInclude, boolean isStopInclude) {
        this.index = index;
        this.start = start;
        this.stop = stop;
        this.isStartInclude = isStartInclude;
        this.isStopInclude = isStopInclude;
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

    public boolean isStartInclude() {
        return isStartInclude;
    }

    public boolean isStopInclude() {
        return isStopInclude;
    }
}
