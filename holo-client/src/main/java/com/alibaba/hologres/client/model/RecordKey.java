/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.impl.util.ShardUtil;

import java.lang.reflect.Array;

/**
 * primary key for a record.
 */
public class RecordKey {
	Record record;

	int hash = 0;
	int[] keys;

	public RecordKey(Record record) {
		this.record = record;
		keys = record.getKeyIndex();
		hash = ShardUtil.hash(record, keys);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RecordKey recordKey = (RecordKey) o;
		if (record == recordKey.record) {
			return true;
		}

		if (record.getKeyIndex().length != recordKey.record.getKeyIndex().length) {
			return false;
		}
		if (record.getKeyIndex().length == 0) {
			return false;
		}
		for (int i : record.getKeyIndex()) {
			Object left = record.getObject(i);
			Object right = recordKey.record.getObject(i);
			Column column = record.getSchema().getColumn(i);
			if (!equals(left, right, !column.isSerial())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	public static boolean equals(Object obj0, Object obj1, boolean isNullEquals) {

		if (obj0 == null) {
			if (obj1 != null) {
				return false;
			} else {
				return isNullEquals;
			}
		} else {
			if (obj1 == null) {
				return false;
			}
		}

		if (obj0.getClass().isArray()) {
			if (obj1.getClass().isArray()) {
				int length0 = Array.getLength(obj0);
				int length1 = Array.getLength(obj1);
				if (length0 != length1) {
					return false;
				} else {
					for (int i = 0; i < length0; ++i) {
						Object child0 = Array.get(obj0, i);
						Object child1 = Array.get(obj1, i);
						if (!equals(child0, child1, true)) {
							return false;
						}
					}
				}
			} else {
				return false;
			}
		} else {
			if (obj1.getClass().isArray()) {
				return false;
			} else {
				return obj0.equals(obj1);
			}
		}
		return true;
	}

	public static int hashCode(Object obj) {
		if (obj == null) {
			return 0;
		}
		if (obj.getClass().isArray()) {
			int hash = 0;
			int length = Array.getLength(obj);
			for (int i = 0; i < length; ++i) {
				Object child = Array.get(obj, i);
				hash = hash * 31 + (child == null ? 0 : child.hashCode());
			}
			return hash;
		} else {
			return obj.hashCode();
		}
	}
}
