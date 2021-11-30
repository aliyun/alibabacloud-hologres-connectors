/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.Worker;
import org.postgresql.model.TableSchema;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;

/**
 * RecordScanner.
 */
public class RecordScanner implements Closeable {
	private ResultSet rs;
	private final byte[] lock;

	private final TableSchema schema;
	private final BitSet selectedColumns;

	public RecordScanner(ResultSet rs, byte[] lock, TableSchema schema, BitSet selectedColumns) {
		this.rs = rs;
		this.lock = lock;
		this.schema = schema;
		this.selectedColumns = selectedColumns;
	}

	public boolean next() throws HoloClientException {
		if (rs != null) {
			try {
				boolean ret = rs.next();
				if (!ret) {
					close();
				}
				return ret;
			} catch (SQLException e) {
				close();
				throw HoloClientException.fromSqlException(e);
			}
		} else {
			throw new HoloClientException(ExceptionCode.ALREADY_CLOSE, "RecordScanner is already close");
		}
	}

	public Record getRecord() throws HoloClientException {
		try {
			Record record = new Record(schema);
			if (selectedColumns == null) {
				for (int i = 0; i < schema.getColumnSchema().length; ++i) {
					Worker.fillRecord(record, i, rs, i + 1, schema.getColumn(i));
				}
			} else {
				int index = 0;
				for (int i = selectedColumns.nextSetBit(0); i >= 0; i = selectedColumns.nextSetBit(i + 1)) {
					Worker.fillRecord(record, i, rs, ++index, schema.getColumn(i));
					if (i == Integer.MAX_VALUE) {
						break; // or (i+1) would overflow
					}
				}
			}
			return record;
		} catch (SQLException e) {
			close();
			throw HoloClientException.fromSqlException(e);
		}
	}

	@Override
	public void close() {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception ignore) {
			} finally {
				rs = null;
			}
			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}

	public boolean isDone() {
		return rs == null;
	}
}
