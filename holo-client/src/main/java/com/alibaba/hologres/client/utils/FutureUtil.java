/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** future相关工具类. */
public class FutureUtil {
    /**
     * 封装了一下异常处理.
     *
     * @param future
     * @param <T>
     * @return
     * @throws HoloClientException
     */
    public static <T> T get(CompletableFuture<T> future) throws HoloClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof HoloClientException) {
                throw (HoloClientException) e.getCause();
            } else if (e.getCause() != null) {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", e.getCause());
            } else {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "", e);
            }
        }
    }
}
