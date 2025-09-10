/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.security.InvalidParameterException;

/** a class to represent a write operation (similar to HBase PUT). */
public class Put {
    Record record;

    /** put类型. */
    public enum MutationType {
        INSERT,
        DELETE
    }

    public Put(TableSchema schema) {
        this.record = Record.build(schema);
        record.setType(MutationType.INSERT);
    }

    public Put(Record record) {
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    /**
     * @param i 列序号(在建表字段中的顺序，从0开始）
     * @param obj 列值
     * @param onlyInsert 当writeMode=INSERT_OR_UPDATE生效。
     *     为true时，这一列只会在主键不存在的情况下写入，如果主键已存在则不更新。常见于记录row创建时间的字段。
     * @return
     */
    public Put setObject(int i, Object obj, boolean onlyInsert) {

        record.setObject(i, obj);
        if (onlyInsert) {
            record.getOnlyInsertColumnSet().set(i);
        }
        return this;
    }

    public Put setObject(int i, Object obj) {
        return setObject(i, obj, false);
    }

    public Put setObject(String columnName, Object obj) {
        return setObject(columnName, obj, false);
    }

    /**
     * @param columnName 列名
     * @param obj 列值
     * @param onlyInsert 当writeMode=INSERT_OR_UPDATE生效。
     *     为true时，这一列只会在主键不存在的情况下写入，如果主键已存在则不更新。常见于记录row创建时间的字段。
     * @return
     */
    public Put setObject(String columnName, Object obj, boolean onlyInsert) {
        Integer i = record.getSchema().getColumnIndex(columnName);
        if (i == null) {
            throw new InvalidParameterException("can not found column named " + columnName);
        }
        setObject(i, obj, onlyInsert);
        return this;
    }

    public Object getObject(int i) {
        return record.getObject(i);
    }

    public boolean isSet(int i) {
        return record.isSet(i);
    }
}
