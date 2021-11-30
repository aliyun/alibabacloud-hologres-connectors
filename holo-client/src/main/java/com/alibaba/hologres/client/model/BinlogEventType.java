package com.alibaba.hologres.client.model;

/**
 * BinlogEventType
 * hg_binlog_event_type： INSERT、DELETE、UPDATE会有对应Binlog
 * 其中UPDATE 操作会产生两条Binlog记录, 一条更新前, 一条更新后的.
 */
public enum BinlogEventType {
	// 表示当前Binlog 为删除一条已有的记录
	DELETE(2),

	// 表示当前Binlog 为更新一条已有的记录中更新前的记录
	BEFORE_UPDATE(3),

	// 表示当前Binlog 为插入一条新的记录
	INSERT(5),

	// 表示当前Binlog 为更新一条已有的记录中更新后的记录
	AFTER_UPDATE(7);

	private final long value;

	BinlogEventType(long value) {
		this.value = value;
	}

	public long getValue() {
		return value;
	}
}
