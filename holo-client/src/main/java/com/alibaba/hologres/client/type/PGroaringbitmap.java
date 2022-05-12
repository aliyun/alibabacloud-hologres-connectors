/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.type;

/**
 * roaringbitmap对象.
 */
public class PGroaringbitmap extends PgDefaultBinaryObject {
	public PGroaringbitmap() {
		super("roaringbitmap");
	}

	String text = null;

	@Override
	public String getValue() {
		if (bytes == null) {
			return null;
		}
		if (text == null) {
			final StringBuilder sb = new StringBuilder("\\x");
			for (int i = 0; i < bytes.length; ++i) {
				int v = bytes[i] & 0xFF;
				String hv = Integer.toHexString(v);
				if (hv.length() < 2) {
					sb.append("0");
				}
				sb.append(hv);
			}
			text = sb.toString();
		}
		return text;
	}
}
