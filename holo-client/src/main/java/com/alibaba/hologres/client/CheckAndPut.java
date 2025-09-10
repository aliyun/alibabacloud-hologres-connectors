/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutRecord;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;

import java.security.InvalidParameterException;

/** a class to represent a write operation (similar to HBase PUT). */
public class CheckAndPut {
    CheckAndPutRecord record;

    public CheckAndPut(
            TableSchema schema,
            String checkColumn,
            CheckCompareOp checkOp,
            Object checkValue,
            Object nullValue) {
        this.record = CheckAndPutRecord.build(schema, checkColumn, checkOp, checkValue, nullValue);
        record.setType(Put.MutationType.INSERT);
    }

    public CheckAndPut(CheckAndPutRecord record) {
        this.record = record;
    }

    public CheckAndPutRecord getRecord() {
        return record;
    }

    /**
     * @param i 列序号(在建表字段中的顺序，从0开始）
     * @param obj 列值
     * @param onlyInsert 当writeMode=INSERT_OR_UPDATE生效。
     *     为true时，这一列只会在主键不存在的情况下写入，如果主键已存在则不更新。常见于记录row创建时间的字段。
     * @return
     */
    public CheckAndPut setObject(int i, Object obj, boolean onlyInsert) {

        record.setObject(i, obj);
        if (onlyInsert) {
            record.getOnlyInsertColumnSet().set(i);
        }
        return this;
    }

    public CheckAndPut setObject(int i, Object obj) {
        return setObject(i, obj, false);
    }

    public CheckAndPut setObject(String columnName, Object obj) {
        return setObject(columnName, obj, false);
    }

    /**
     * @param columnName 列名
     * @param obj 列值
     * @param onlyInsert 当writeMode=INSERT_OR_UPDATE生效。
     *     为true时，这一列只会在主键不存在的情况下写入，如果主键已存在则不更新。常见于记录row创建时间的字段。
     * @return
     */
    public CheckAndPut setObject(String columnName, Object obj, boolean onlyInsert) {
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
