/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.model.Record;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadLocalRandom;

/**
 * shard相关的工具方法.
 */
public class ShardUtil {

	private static final Charset UTF8 = Charset.forName("UTF-8");
	public static final String NAME = "HASH";
	public static final HashFunction HASH = Hashing.murmur3_32(104729);

	public static final int RANGE_START = 0;
	public static final int RANGE_END = 65536;

	public static final int NULL_HASH_CODE = Integer.remainderUnsigned(hash(""), RANGE_END);

	public static int hash(Record record, int[] indexes) {
		int hash = 0;
		boolean first = true;
		if (indexes == null || indexes.length == 0) {
			ThreadLocalRandom rand = ThreadLocalRandom.current();
			hash = rand.nextInt();
		} else {
			for (int i : indexes) {
				if (first) {
					hash = ShardUtil.hash(record.getObject(i));
				} else {
					hash ^= ShardUtil.hash(record.getObject(i));
				}
				first = false;
			}
		}
		return hash;
	}

	public static int hash(Object obj) {
		if (obj == null) {
			return NULL_HASH_CODE;
		} else {
			if (obj instanceof byte[]) {
				return HASH.hashBytes((byte[]) obj).asInt();
			} else if (obj.getClass().isArray()) {
				int hash = 0;
				int length = Array.getLength(obj);
				for (int i = 0; i < length; ++i) {
					Object child = Array.get(obj, i);
					hash = hash * 31 + (child == null ? 0 : hash(child));
				}
				return hash;
			} else {
				return hash(String.valueOf(obj).getBytes(UTF8));
			}
		}
	}

	public static int[][] split(int n) {
		int base = RANGE_END / n;
		int remain = RANGE_END % n;
		int start = 0;
		int end = 0;
		int[][] ret = new int[n][];
		for (int i = 0; i < n; ++i) {
			end = start + base + ((remain > 0) ? 1 : 0);
			if (remain > 0) {
				--remain;
			}
			ret[i] = new int[]{start, end};
			start = end;
		}
		return ret;
	}
}
