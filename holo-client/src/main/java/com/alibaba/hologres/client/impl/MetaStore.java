/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;

/** 元信息缓存. */
public class MetaStore {
    public final Cache<TableName, TableSchema> tableCache;
    public final Cache<TableName, Cache<String, Partition>> partitionCache;

    public MetaStore(long tableCacheTTL) {
        this.tableCache = new Cache<>(tableCacheTTL, null);
        this.partitionCache = new Cache<>((tableName) -> new Cache<>());
    }
}
