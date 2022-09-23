/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

/**
 * 用于返回copy行数.
 */
public interface WithCopyResult {
	long getResult();
}
