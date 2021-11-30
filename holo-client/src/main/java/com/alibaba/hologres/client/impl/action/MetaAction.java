/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import org.postgresql.model.TableName;
import org.postgresql.model.TableSchema;

/**
 * ma.
 */
public class MetaAction extends AbstractAction<TableSchema> {

	TableName tableName;
	int mode;

	/**
	 * @param tableName 表名
	 * @param mode @see org.postgresql.util.cache.Cache
	 */
	public MetaAction(TableName tableName, int mode) {
		this.tableName = tableName;
		this.mode = mode;
	}

	public TableName getTableName() {
		return tableName;
	}

	public int getMode() {
		return mode;
	}
}
