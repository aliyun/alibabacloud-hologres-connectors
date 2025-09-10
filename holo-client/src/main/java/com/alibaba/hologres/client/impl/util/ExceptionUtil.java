/*
 * Copyright (c) 2023. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;

/** 异常相关工具类. */
public class ExceptionUtil {
    /**
     * 当a和b同为HoloClientWithDetailsException时，将b合并入a对象，否则返回任何一个异常.
     *
     * @param a
     * @param b
     * @return
     */
    public static HoloClientException merge(HoloClientException a, HoloClientException b) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        } else if (a instanceof HoloClientWithDetailsException) {
            if (b instanceof HoloClientWithDetailsException) {
                ((HoloClientWithDetailsException) a).merge((HoloClientWithDetailsException) b);
                return a;
            } else {
                return b;
            }
        } else {
            return a;
        }
    }
}
