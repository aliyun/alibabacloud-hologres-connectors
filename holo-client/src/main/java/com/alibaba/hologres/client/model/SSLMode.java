/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

/**
 * enum for ssl mode.
 * see SSL_MODE in org.postgresql.PGProperty.
 */
public enum SSLMode {
	// disable
	DISABLE("disable"),
	// require
	REQUIRE("require"),
	// verify-ca
	VERIFY_CA("verify-ca"),
	// verify-full
	VERIFY_FULL("verify-full");

	private final String pgPropertyValue;

	SSLMode(String pgPropertyValue) {
		this.pgPropertyValue = pgPropertyValue;
	}

	public static SSLMode fromPgPropertyValue(String pgPropertyValue) {
		for (SSLMode sslMode : SSLMode.values()) {
			if (sslMode.getPgPropertyValue().equalsIgnoreCase(pgPropertyValue)) {
				return sslMode;
			}
		}
		throw new IllegalArgumentException("invalid ssl mode: " + pgPropertyValue);
	}

	public String getPgPropertyValue() {
		return pgPropertyValue;
	}
}
