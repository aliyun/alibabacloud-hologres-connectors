package com.alibaba.hologres.client.model.checkandput;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.Serializable;

/**
 * CheckAndPut Record, 包含需要check的字段，条件等.
 */
public class CheckAndPutRecord extends Record implements Serializable {

	CheckAndPutCondition checkAndPutCondition;

	public CheckAndPutRecord(TableSchema schema, String checkColumnName, CheckCompareOp checkOp, Object checkValue, Object nullValue) {
		super(schema);
		this.checkAndPutCondition = new CheckAndPutCondition(checkColumnName, checkOp, checkValue, nullValue);
	}

	public CheckAndPutRecord(Record record, CheckAndPutCondition checkAndPutCondition) {
		super(record.getSchema());
		this.merge(record);
		this.setType(record.getType());
		this.checkAndPutCondition = checkAndPutCondition;
	}

	public static CheckAndPutRecord build(TableSchema schema, String checkColumn, CheckCompareOp checkOp, Object checkValue, Object nullValue) {
		return new CheckAndPutRecord(schema, checkColumn, checkOp, checkValue, nullValue);
	}

	public CheckAndPutCondition getCheckAndPutCondition() {
		return checkAndPutCondition;
	}

	@Override
	public String toString() {
		return checkAndPutCondition.toString() + super.toString();
	}
}
