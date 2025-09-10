/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

/** EqualsFilter. */
public class EqualsFilter implements Filter {
    int index;
    Object obj;

    public EqualsFilter(int index, Object obj) {
        this.index = index;
        this.obj = obj;
    }

    public int getIndex() {
        return index;
    }

    public Object getObj() {
        return obj;
    }
}
