/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import java.io.Serializable;
import java.sql.Types;
import java.util.Objects;

/** class for a Hologres column. */
public class Column implements Serializable {
    private String name;
    private String typeName;
    private int type;
    private String comment;
    private Boolean allowNull;
    private Boolean isPrimaryKey;
    private Object defaultValue;
    private Boolean isArrayType;
    private int arrayElementType;
    private int precision;
    private int scale;
    private Boolean isGeneratedColumn = false;

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

    public Boolean isArrayType() {
        return isArrayType;
    }

    public void setArrayType(Boolean arrayType) {
        this.isArrayType = arrayType;
    }

    public void setArrayElementType(String elementTypeName) {
        switch (elementTypeName) {
            case "char":
                this.arrayElementType = Types.CHAR;
                break;
            case "int2":
                this.arrayElementType = Types.SMALLINT;
                break;
            case "int4":
                this.arrayElementType = Types.INTEGER;
                break;
            case "int8":
                this.arrayElementType = Types.BIGINT;
                break;
            case "float4":
                this.arrayElementType = Types.REAL;
                break;
            case "float8":
                this.arrayElementType = Types.DOUBLE;
                break;
            case "bool":
                this.arrayElementType = Types.BIT;
                break;
            case "text":
            case "varchar":
                this.arrayElementType = Types.VARCHAR;
                break;
            default:
        }
    }

    public int getArrayElementType() {
        return arrayElementType;
    }

    public boolean isSerial() {
        return "serial".equals(typeName)
                || "bigserial".equals(typeName)
                || "smallserial".equals(typeName);
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

    public void setGeneratedColumn(Boolean isGeneratedColumn) {
        this.isGeneratedColumn = isGeneratedColumn;
    }

    public Boolean isGeneratedColumn() {
        return isGeneratedColumn;
    }

    @Override
    public String toString() {
        return "\nColumn{"
                + "name='"
                + name
                + '\''
                + ", typeName='"
                + typeName
                + '\''
                + ((Objects.nonNull(allowNull) && !allowNull) ? ", not null" : "")
                + ((Objects.nonNull(defaultValue)) ? ", default value=" + defaultValue : "")
                + ((Objects.nonNull(isPrimaryKey) && isPrimaryKey) ? ", primary key" : "")
                + ((Objects.nonNull(isGeneratedColumn) && isGeneratedColumn)
                        ? ", generated column"
                        : "")
                + ((Objects.nonNull(comment) && !comment.isEmpty()) ? ", comment=" + comment : "")
                + '}';
    }
}
