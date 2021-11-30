/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 长度为1的队列.
 *
 * @param <T> 类型
 */
public class ObjectChan<T> {

	T value;
	private final ReentrantLock lock;
	private final Condition notEmpty;

	public ObjectChan() {
		lock = new ReentrantLock(false);
		notEmpty = lock.newCondition();
	}

	public boolean set(T t) {
		lock.lock();
		try {
			if (value == null) {
				value = t;
				notEmpty.signalAll();
				return true;
			} else {
				return false;
			}
		} finally {
			lock.unlock();
		}
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (value == null) {
				if (nanos <= 0) {
					return null;
				}
				nanos = notEmpty.awaitNanos(nanos);
			}
			return value;
		} finally {
			lock.unlock();
		}
	}

	public void clear() {
		lock.lock();
		try {
			value = null;
		} finally {
			lock.unlock();
		}
	}
}
