package com.alibaba.hologres.hive.conf;

/**
 * HoloStorageConfig.
 */
public enum HoloStorageConfig {
	TABLE("table", true),
	USERNAME("username", false),
	PASSWORD("password", false),

	JDBC_URL("jdbc.url", true),
	JDBC_WRITE_SIZE("jdbc.write.size", false),
	JDBC_WRITE_BYTE_SIZE("jdbc.write.byteSize", false);

	private String propertyName;
	private boolean required = false;

	HoloStorageConfig(String propertyName, boolean required) {
		this.propertyName = propertyName;
		this.required = required;
	}

	HoloStorageConfig(String propertyName) {
		this.propertyName = propertyName;
	}

	public String getPropertyName() {
		return "hive.sql." + this.propertyName;
	}

	public boolean isRequired() {
		return this.required;
	}
}
