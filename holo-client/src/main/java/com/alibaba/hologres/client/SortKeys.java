/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

/**
 * Scan操作的排序keys.
 */
public enum SortKeys {
	PRIMARY_KEY,
	CLUSTERING_KEY,
	NONE,
}
