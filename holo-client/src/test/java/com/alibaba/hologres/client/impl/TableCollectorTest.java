/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.collector.TableCollector;
import com.alibaba.hologres.client.model.OnConflictAction;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** UT for TableCollector. */
public class TableCollectorTest extends HoloClientTestBase {
    /** ExecutionPool Method: put(Put put). */
    @Test
    public void testTableCollector001() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        config.setDynamicPartition(true);
        config.setConnectionMaxIdleMs(10000L);
        config.setAppName("tableCollector001");
        try (ExecutionPool pool =
                ExecutionPool.buildOrGet("tableCollector001ExecutionPool", config, false)) {
            TableCollector tableCollector = new TableCollector(config, pool);

            AtomicBoolean running = new AtomicBoolean(true);
            AtomicBoolean failed = new AtomicBoolean(false);
            ExecutorService es =
                    new ThreadPoolExecutor(
                            20,
                            20,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(20),
                            r -> {
                                Thread t = new Thread(r);
                                return t;
                            },
                            new ThreadPoolExecutor.AbortPolicy());

            // resize和getByteSize同时执行
            Runnable resize =
                    () -> {
                        try {
                            while (running.get()) {
                                tableCollector.resize(new Random().nextInt(10) + 1);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            failed.set(true);
                        }
                    };
            Runnable getByteSize =
                    () -> {
                        try {
                            while (running.get()) {
                                tableCollector.getByteSize();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            failed.set(true);
                        }
                    };

            for (int i = 0; i < 10; ++i) {
                es.execute(resize);
                es.execute(getByteSize);
            }
            Thread.sleep(2000);
            running.set(false);

            es.shutdown();
            while (!es.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {}

            if (failed.get()) {
                Assert.fail();
            }
        }
    }
}
