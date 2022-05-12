/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import java.io.Serializable;

/**
 * class for a Hologres column.
 */
public class Column implements Serializable {
	private String name;
	private String typeName;
	private int type;
	private String comment;
	private Boolean allowNull;
	private Boolean isPrimaryKey;
	private Object defaultValue;
	private Boolean arrayType;
	private int precision;
	private int scale;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public Boolean getAllowNull() {
		if (allowNull == null) {
			return false;
		}
		return allowNull;
	}

	public void setAllowNull(Boolean allowNull) {
		this.allowNull = allowNull;
	}

	public Boolean getPrimaryKey() {
		if (isPrimaryKey == null) {
			return false;
		}
		return isPrimaryKey;
	}

	public void setPrimaryKey(Boolean primaryKey) {
		isPrimaryKey = primaryKey;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public Boolean getArrayType() {
		return arrayType;
	}

	public void setArrayType(Boolean arrayType) {
		this.arrayType = arrayType;
	}

	public boolean isSerial() {
		return "serial".equals(typeName) || "bigserial".equals(typeName) || "smallserial".equals(typeName);
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public int getScale() {
		return scale;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}
}
