/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.action.AbstractAction;
import com.alibaba.hologres.client.impl.action.CopyAction;
import com.alibaba.hologres.client.impl.action.EmptyAction;
import com.alibaba.hologres.client.impl.action.GetAction;
import com.alibaba.hologres.client.impl.action.MetaAction;
import com.alibaba.hologres.client.impl.action.PutAction;
import com.alibaba.hologres.client.impl.action.ScanAction;
import com.alibaba.hologres.client.impl.action.SqlAction;
import com.alibaba.hologres.client.impl.binlog.action.BinlogAction;
import com.alibaba.hologres.client.impl.binlog.handler.BinlogActionHandler;
import com.alibaba.hologres.client.impl.handler.ActionHandler;
import com.alibaba.hologres.client.impl.handler.CopyActionHandler;
import com.alibaba.hologres.client.impl.handler.EmptyActionHandler;
import com.alibaba.hologres.client.impl.handler.GetActionHandler;
import com.alibaba.hologres.client.impl.handler.MetaActionHandler;
import com.alibaba.hologres.client.impl.handler.PutActionHandler;
import com.alibaba.hologres.client.impl.handler.ScanActionHandler;
import com.alibaba.hologres.client.impl.handler.SqlActionHandler;
import com.alibaba.hologres.client.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * worker.
 */
public class Worker implements Runnable {
	public static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

	final ConnectionHolder connectionHolder;
	ObjectChan<AbstractAction> recordCollector = new ObjectChan<>();
	final AtomicBoolean started;
	final HoloConfig config;
	AtomicReference<Throwable> fatal = new AtomicReference<>(null);
	private final String name;
	Map<Class, ActionHandler> handlerMap = new HashMap<>();

	public Worker(HoloConfig config, AtomicBoolean started, int index, boolean isShardEnv) {
		this(config, started, index, isShardEnv, false);
	}

	public Worker(HoloConfig config, AtomicBoolean started, int index, boolean isShardEnv, boolean isFixed) {
		this.config = config;
		connectionHolder = new ConnectionHolder(config, this, isShardEnv, isFixed);
		this.started = started;
		this.name = (isFixed ? "Fixed-" : "") + "Worker-" + index;
		handlerMap.put(EmptyAction.class, new EmptyActionHandler(config));
		handlerMap.put(GetAction.class, new GetActionHandler(connectionHolder, config));
		handlerMap.put(MetaAction.class, new MetaActionHandler(connectionHolder, config));
		handlerMap.put(SqlAction.class, new SqlActionHandler(connectionHolder, config));
		handlerMap.put(CopyAction.class, new CopyActionHandler(connectionHolder, config));
		handlerMap.put(PutAction.class, new PutActionHandler(connectionHolder, config));
		handlerMap.put(ScanAction.class, new ScanActionHandler(connectionHolder, config));
		handlerMap.put(BinlogAction.class, new BinlogActionHandler(started, config, isShardEnv));
	}

	public boolean offer(AbstractAction action) throws HoloClientException {
		if (fatal.get() != null) {
			throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "fatal", fatal.get());
		}
		if (action != null) {
			if (!started.get()) {
				throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "worker is close");
			}
			return this.recordCollector.set(action);
		} else {
			return this.recordCollector.set(new EmptyAction());
		}
	}

	protected  <T extends AbstractAction> void handle(T action) throws HoloClientException {
		String metricsName = null;
		long start = System.nanoTime();
		try {
			ActionHandler<T> handler = handlerMap.get(action.getClass());
			if (handler == null) {
				throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "Unknown action:" + action.getClass().getName());
			}
			metricsName = handler.getCostMsMetricName();
			handler.handle(action);
		} catch (Throwable e) {
			if (action.getFuture() != null && !action.getFuture().isDone()) {
				action.getFuture().completeExceptionally(e);
			}
			throw e;
		} finally {
			long end = System.nanoTime();
			long cost = (end - start) / 1000000L;
			if (metricsName != null) {
				Metrics.registry().meter(metricsName).mark(cost);
			}
			Metrics.registry().meter(Metrics.METRICS_ALL_COST_MS_ALL).mark(cost);
		}
	}

	@Override
	public void run() {
		LOGGER.info("worker:{} start", this);
		while (started.get()) {
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
						if (action.getSemaphore() != null) {
							action.getSemaphore().release();
						}

					}
				}
				if (System.currentTimeMillis() - connectionHolder.getLastActiveTs() > config.getConnectionMaxIdleMs()) {
					connectionHolder.close();
				}
			} catch (Throwable e) {
				LOGGER.error("should not happen", e);
				fatal.set(e);
				break;
			}

		}
		LOGGER.info("worker:{} stop", this);
		connectionHolder.close();

	}

	@Override
	public String toString() {
		return name;
	}
}
