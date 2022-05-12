package com.alibaba.hologres.client.impl.binlog;

import java.security.InvalidParameterException;

/**
 * BinlogEventType
 * hg_binlog_event_type： INSERT、DELETE、UPDATE会有对应Binlog
 * 其中UPDATE 操作会产生两条Binlog记录, 一条更新前, 一条更新后的.
 */
public enum BinlogEventType {
	// 客户端的一个虚拟eventType，引擎中并不存在.
	HeartBeat(-1),

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

	public static BinlogEventType of(long value) {
		switch ((int) value) {
			case 2:
				return DELETE;
			case 3:
				return BEFORE_UPDATE;
			case 5:
				return INSERT;
			case 7:
				return AFTER_UPDATE;
			default:
				throw new InvalidParameterException("unknow binlog event type" + value);
		}
	}
}
