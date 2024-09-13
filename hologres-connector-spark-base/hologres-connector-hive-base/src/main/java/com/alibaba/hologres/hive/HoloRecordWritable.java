package com.alibaba.hologres.hive;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.hive.exception.HiveHoloStorageException;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/** HoloRecordWritable. */
public class HoloRecordWritable implements Writable {

    private Object[] columnValues;
    private String[] columnNames;

    public void clear() {
        Arrays.fill(columnValues, null);
    }

    public HoloRecordWritable(int numColumns, String[] columnNames) {
        this.columnValues = new Object[numColumns];
        this.columnNames = columnNames;
    }

    public void set(int i, Object columnObject) {
        columnValues[i] = columnObject;
    }

    public void write(Put put) throws HiveHoloStorageException {
        if (columnValues == null) {
            throw new HiveHoloStorageException("No data available to be written");
        }
        for (int i = 0; i < columnValues.length; i++) {
            Object value = columnValues[i];
            put.setObject(columnNames[i], value);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {}

    @Override
    public void readFields(DataInput dataInput) throws IOException {}

    public Object[] getColumnValues() {
        return columnValues;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
