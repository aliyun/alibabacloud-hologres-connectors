package com.alibaba.hologres.hive.input;

import com.alibaba.hologres.client.model.TableSchema;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** HoloInputSplit. */
public class HoloInputSplit implements InputSplit {

    private static final String[] EMPTY_ARRAY = new String[] {};

    private final int startShard;
    private final int endShard;
    private final TableSchema schema;

    public HoloInputSplit(int startShard, int endShard, TableSchema schema) {
        this.startShard = startShard;
        this.endShard = endShard;
        this.schema = schema;
    }

    @Override
    public void write(DataOutput out) throws IOException {}

    @Override
    public void readFields(DataInput in) throws IOException {}

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return EMPTY_ARRAY;
    }

    public int getStartShard() {
        return startShard;
    }

    public int getEndShard() {
        return endShard;
    }

    public TableSchema getSchema() {
        return schema;
    }
}
