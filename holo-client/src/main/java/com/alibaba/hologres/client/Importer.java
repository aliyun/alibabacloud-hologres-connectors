/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;

import java.io.InputStream;
import java.security.InvalidParameterException;

/**
 * Exporter.
 */
public class Importer {
	private TableSchema schema;
	private int startShardId = -1;
	private int endShardId = -1;
	private InputStream is;
	private int bufferSize = -1;
	private int threadSize = 1;

	Importer(TableSchema schema, int startShardId, int endShardId, InputStream is, int threadSize, int bufferSize) {
		this.schema = schema;
		this.startShardId = startShardId;
		this.endShardId = endShardId;
		this.is = is;
		this.threadSize = threadSize;
		this.bufferSize = bufferSize;
	}

	public TableSchema getSchema() {
		return schema;
	}

	public int getStartShardId() {
		return startShardId;
	}

	public int getEndShardId() {
		return endShardId;
	}

	public InputStream getInputStream() {
		return is;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public int getThreadSize() {
		return threadSize;
	}

	public static Importer.Builder newBuilder(TableSchema schema) {
		return new Importer.Builder(schema);
	}

	/**
	 * builder.
	 */
	public static class Builder {
		private TableSchema schema;
		private int startShardId = -1;
		private int endShardId = -1;
		private InputStream is;
		private int bufferSize = -1;
		private int threadSize = 1;

		public Builder(TableSchema schema) {
			this.schema = schema;
		}

		public Builder setShardRange(int shardId) {
			setShardRange(shardId, shardId + 1);
			return this;
		}

		/**
		 * 输出shardId [start,end) 的数据.
		 *
		 * @param startShardId start
		 * @param endShardId   end
		 * @return
		 */
		public Builder setShardRange(int startShardId, int endShardId) {
			if (endShardId <= startShardId) {
				throw new InvalidParameterException("startShardId must less then endShardId");
			}
			this.startShardId = startShardId;
			this.endShardId = endShardId;
			return this;
		}

		public Builder setInputStream(InputStream is) {
			this.is = is;
			this.threadSize = 1;
			return this;
		}

		public Builder setBufferSize(int size) {
			this.bufferSize = size;
			return this;
		}

		public Builder setThreadSize(int size) {
			if (this.is == null) {
				this.threadSize = size;
			}
			return this;
		}

		public Importer build() {
			return new Importer(schema, startShardId, endShardId, is, threadSize, bufferSize);
		}
	}
}
