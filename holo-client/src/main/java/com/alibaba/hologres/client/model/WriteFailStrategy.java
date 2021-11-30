/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

/**
 * 写入失败时的策略.
 * TRY_ONE_BY_ONE，会将攒批提交退化为逐条提交
 * NONE，抛异常，什么都不做
 */
public enum WriteFailStrategy {
	TRY_ONE_BY_ONE,
	NONE
}
