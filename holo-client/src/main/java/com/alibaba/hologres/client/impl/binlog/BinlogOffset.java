package com.alibaba.hologres.client.impl.binlog;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * BinlogOffset
 * 用于为每个shard单独设置读取binlog的起始点.
 */
public class BinlogOffset {
	/**
	 * sequence 即 hg_binlog_lsn.
	 */
	private long sequence;

	/**
	 * timestamp 对应 hg_binlog_timestamp_us.
	 */
	private long timestamp;

	private String startTimeText;

	public BinlogOffset() {
		this(-1, -1);
	}

	public BinlogOffset(long sequence, long timestamp) {
		this.sequence = sequence;
		this.timestamp = timestamp;
		this.startTimeText = Timestamp.from(Instant.ofEpochMilli(timestamp / 1000L)).toString();
	}

	public long getSequence() {
		return sequence;
	}

	public BinlogOffset setSequence(long sequence) {
		this.sequence = sequence;
		return this;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getStartTimeText() {
		return startTimeText;
	}

	public BinlogOffset setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	public BinlogOffset setTimestamp(String timestamp) {
		this.startTimeText = timestamp;
		if (timestamp != null) {
			this.timestamp = Timestamp.valueOf(timestamp).getTime() * 1000L;
		}
		return this;
	}

	/**
	 * lsn和timestamp至少有一个大于-1才表示有效的进行了offset的设置.
	 */
	public boolean isValid() {
		return sequence > -1 || timestamp > -1;
	}

	public boolean hasSequence() {
		return sequence > -1;
	}

	public boolean hasTimestamp() {
		return timestamp > -1;
	}

	public BinlogOffset next() {
		return new BinlogOffset(this.sequence >= 0 ? this.sequence + 1 : -1, this.timestamp);
	}

	@Override
	public String toString() {
		return "(" + sequence + ", " + timestamp + ")";
	}
}
