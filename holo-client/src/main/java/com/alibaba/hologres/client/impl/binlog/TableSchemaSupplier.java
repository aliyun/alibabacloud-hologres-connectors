/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.binlog;

import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.TableSchema;

/**
 * 提供TableSchema.
 */
public interface TableSchemaSupplier {
	TableSchema apply() throws HoloClientException;
}
