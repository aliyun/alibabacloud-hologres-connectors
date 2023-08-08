package com.alibaba.hologres.client.impl.binlog;


/**
 * BinlogLevel
 * hologres物理表的binlog level，对应"binlog.level" table property.
 */
public enum BinlogLevel {
	// binlog.level设置为replica，表示holo物理表开启了binlog
	REPLICA,

	// 表示没有设置binlog.level属性
	NONE
}
