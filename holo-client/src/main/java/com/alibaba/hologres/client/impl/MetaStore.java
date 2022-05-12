/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;

/**
 * 元信息缓存.
 */
public class MetaStore {
	public Cache<TableName, TableSchema> tableCache = new Cache<>();
	public Cache<TableName, Cache<String, Partition>> partitionCache = new Cache<>((tableName) -> new Cache<>());
}
