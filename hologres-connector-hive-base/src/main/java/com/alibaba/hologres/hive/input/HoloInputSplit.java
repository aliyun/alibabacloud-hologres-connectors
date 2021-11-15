package com.alibaba.hologres.hive.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** HoloInputSplit. */
public class HoloInputSplit extends FileSplit implements InputSplit {

    private static final String[] EMPTY_ARRAY = new String[] {};

    private int limit = 0;
    private int offset = 0;

    public HoloInputSplit() {
        super((Path) null, 0, 0, EMPTY_ARRAY);
    }

    public HoloInputSplit(long start, long end, Path dummyPath) {
        super(dummyPath, 0, 0, EMPTY_ARRAY);
        this.setLimit((int) start);
        this.setOffset((int) end);
    }

    public HoloInputSplit(int limit, int offset) {
        super((Path) null, 0, 0, EMPTY_ARRAY);
        this.limit = limit;
        this.offset = offset;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(limit);
        out.writeInt(offset);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        limit = in.readInt();
        offset = in.readInt();
    }

    @Override
    public long getLength() {
        return limit;
    }

    @Override
    public String[] getLocations() throws IOException {
        return EMPTY_ARRAY;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
