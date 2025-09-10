/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.impl.binlog.handler.BinlogActionHandler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 只跑一次的worker，没有while. */
public class OneshotWorker extends Worker {

    public OneshotWorker(
            HoloConfig config,
            AtomicBoolean started,
            String index,
            boolean isShadingEnv,
            boolean isFixed) {
        super(config, started, index, isShadingEnv, isFixed);
        handlerMap.put(
                BinlogAction.class,
                new BinlogActionHandler(started, config, isShadingEnv, isFixed));
    }

    @Override
    public void run() {
        LOGGER.info("worker:{} start", this);
        try {
            AbstractAction action = recordCollector.get(2000L, TimeUnit.MILLISECONDS);
            /*
             * 每个循环做2件事情：
             * 1 有action就执行action
             * 2 根据connectionMaxIdleMs释放空闲connection
             * */
            if (null != action) {
                try {
                    handle(action);
                } finally {
                    recordCollector.clear();
                }
            } else {

            }
        } catch (Throwable e) {
            LOGGER.error("should not happen", e);
        }
        LOGGER.info("worker:{} stop", this);
        connectionHolder.close();
    }
}
