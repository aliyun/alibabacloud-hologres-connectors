package com.alibaba.hologres.client.model;

/**
 * BinlogOffset
 * 用于为每个shard单独设置读取binlog的起始点.
 */
public class BinlogOffset {
	/** sequence 即 hg_binlog_lsn. */
	private long sequence;

	/** timestamp 即 hg_binlog_timestampUs. */
	private long timestamp;

	public BinlogOffset() {
		this(-1, -1);
	}

	public BinlogOffset(long sequence, long timestamp) {
		this.sequence = sequence;
		this.timestamp = timestamp;
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

	public BinlogOffset setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		return this;
	}

	/** lsn和timestamp至少有一个大于0才表示有效的进行了offset的设置. */
	public boolean isInvalid() {
		return sequence <= 0 && timestamp <= 0;
	}

	public boolean hasSequence() {
		return sequence > 0;
	}

	public boolean hasTimestamp() {
		return timestamp > 0;
	}

	public BinlogOffset next() {
		return new BinlogOffset(this.sequence >= 0 ? this.sequence + 1 : -1, this.timestamp);
	}

	@Override
	public String toString() {
		return "(" + sequence + ", " + timestamp + ")";
	}
}
