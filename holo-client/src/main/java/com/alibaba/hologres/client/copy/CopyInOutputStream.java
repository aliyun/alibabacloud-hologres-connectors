/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.copy;

import org.postgresql.copy.CopyIn;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * 把CopyIn封装成一个OutputStream.
 */
public class CopyInOutputStream extends OutputStream implements WithCopyResult {
	private final CopyIn copyIn;
	private long result = -1;

	public CopyInOutputStream(CopyIn copyIn) {
		this.copyIn = copyIn;
	}

	@Override
	public void write(int b) throws IOException {
		throw new UnsupportedOperationException("please use void write(byte b[], int off, int len)");
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException();
		} else if ((off < 0) || (off > b.length) || (len < 0) ||
				((off + len) > b.length) || ((off + len) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return;
		}
		try {
			copyIn.writeToCopy(b, off, len);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void flush() throws IOException {
		try {
			copyIn.flushCopy();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public long getResult() {
		return result;
	}

	@Override
	public void close() throws IOException {
		try {
			result = copyIn.endCopy();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
}
