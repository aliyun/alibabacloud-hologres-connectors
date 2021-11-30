/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import com.alibaba.hologres.client.impl.copy.CopyContext;
import org.postgresql.model.TableSchema;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

/**
 * CopyAction,支持基于shardId的CopyIn和CopyOut.
 */
public class CopyAction extends AbstractAction<Long> {

	private TableSchema schema;

	int startShardId = -1;
	int endShardId = -1;

	CompletableFuture<CopyContext> readyToStart = new CompletableFuture<>();

	Mode mode;

	//mode = OUT时的成员变量
	private OutputStream os;

	//mode = IN时的成员变量
	private InputStream is;
	private int bufferSize = -1; // in的时候需要一个buffer从InputStream中获取bytes后写入socket的OutputStream

	/**
	 * Copy类型.
	 */
	public enum Mode {
		IN,
		OUT
	}

	public CopyAction(TableSchema schema, OutputStream os, InputStream is, int startShardId, int endShardId, Mode mode) {
		this.schema = schema;
		this.os = os;
		this.is = is;
		this.startShardId = startShardId;
		this.endShardId = endShardId;
		this.mode = mode;
	}

	public Mode getMode() {
		return mode;
	}

	public TableSchema getSchema() {
		return schema;
	}

	public OutputStream getOs() {
		return os;
	}

	public InputStream getIs() {
		return is;
	}

	public int getStartShardId() {
		return startShardId;
	}

	public int getEndShardId() {
		return endShardId;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public CompletableFuture<CopyContext> getReadyToStart() {
		return readyToStart;
	}
}
