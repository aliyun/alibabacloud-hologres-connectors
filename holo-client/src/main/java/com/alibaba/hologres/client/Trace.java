/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于debug性能追踪.
 */
public class Trace {

	long[] values = new long[20];
	String[] text = new String[20];

	AtomicInteger index = new AtomicInteger(1);

	public void begin() {
		values[0] = System.nanoTime();
		text[0] = "start";
	}

	public void step(String name) {
		long current = System.nanoTime();
		int index = this.index.getAndIncrement();
		if (index < values.length - 1) {
			values[index] = current;
			text[index] = name;
		}
	}

	public long getCost() {
		int index = this.index.get();
		return (values[index - 1] - values[0]) / 1000L;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int index = this.index.get();
		for (int i = 1; i < index; ++i) {
			sb.append(text[i]).append(":").append((values[i] - values[i - 1]) / 1000L).append(" us -> ");
		}
		sb.append("total:").append(getCost()).append(" us");
		return sb.toString();
	}
}
