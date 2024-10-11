package com.alibaba.hologres.client.copy;

/**
 * enum for write mode.
 * */
public enum CopyMode {
	// fixed copy
	STREAM,
	// 批量copy
	BULK_LOAD,
	// bulkLoad 自Hologres 2.2.25版本起支持on_conflict, 为了兼容历史版本, 分为两种模式
	BULK_LOAD_ON_CONFLICT
}
