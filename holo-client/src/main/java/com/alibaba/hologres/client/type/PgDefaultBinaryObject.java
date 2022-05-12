/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.type;

import org.postgresql.util.PGBinaryObject;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

/**
 * 通用的PgBinaryObject实现.
 */
public class PgDefaultBinaryObject extends PGobject implements PGBinaryObject {

	int length = 0;
	byte[] bytes = null;

	public PgDefaultBinaryObject(String type) {
		this.type = type;
	}

	@Override
	public void setByteValue(byte[] bytes, int i) throws SQLException {
		length = bytes.length - i;
		if (this.bytes == null || this.bytes.length < length) {
			this.bytes = new byte[length];
		}
		System.arraycopy(bytes, i, this.bytes, 0, length);
	}

	@Override
	public int lengthInBytes() {
		return length;
	}

	@Override
	public void toBytes(byte[] bytes, int i) {
		System.arraycopy(this.bytes, 0, bytes, i, length);
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		PgDefaultBinaryObject ret = new PgDefaultBinaryObject(type);
		ret.length = length;
		ret.bytes = new byte[length];
		System.arraycopy(this.bytes, 0, ret.bytes, 0, length);
		return ret;
	}

	@Override
	public String getValue() {
		throw new UnsupportedOperationException("Unsupported type " + type + " input byte[]");
	}
}
