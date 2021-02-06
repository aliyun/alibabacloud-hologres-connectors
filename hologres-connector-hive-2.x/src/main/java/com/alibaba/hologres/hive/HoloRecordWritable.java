package com.alibaba.hologres.hive;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * HoloRecordWritable.
 */
public class HoloRecordWritable implements Writable {

	private Object[] columnValues;

	public void clear() {
		Arrays.fill(columnValues, null);
	}

	public HoloRecordWritable(int numColumns) {
		this.columnValues = new Object[numColumns];
	}

	public void set(int i, Object columnObject) {
		columnValues[i] = columnObject;
	}

	public void write(Put put) throws HiveHoloStorageException {
		if (columnValues == null) {
			throw new HiveHoloStorageException("No data available to be written");
		}
		if (put.getRecord().getSchema().getColumns().length != columnValues.length) {
			throw new HiveHoloStorageException("expect columns " + put.getRecord().getSchema().getColumns().length + " but " + columnValues.length);
		}
		for (int i = 0; i < columnValues.length; i++) {
			Object value = columnValues[i];
			put.setObject(i, value);
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {

	}
}
