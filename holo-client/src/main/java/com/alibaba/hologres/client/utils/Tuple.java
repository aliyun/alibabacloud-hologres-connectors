/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import java.util.Objects;

/**
 * 元组.
 *
 * @param <L>
 * @param <R>
 */
public class Tuple<L, R> {
    public L l;
    public R r;

    public Tuple(L l, R r) {
        this.l = l;
        this.r = r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(l, tuple.l) && Objects.equals(r, tuple.r);
    }

    @Override
    public int hashCode() {
        return Objects.hash(l, r);
    }

    @Override
    public String toString() {
        return "Tuple{" + "l=" + l + ", r=" + r + '}';
    }
}
