package com.alibaba.hologres.shipper.generic;

import java.io.Closeable;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;

public abstract class AbstractTable implements Closeable {
    public abstract String getTableDDL(boolean hasToolkit);
    //return table DDL as a  string
    public abstract void setTableDDL(String DDLInfo) throws Exception;
    //set table DDL according to DDLInfo and user requirements
    public abstract void readTableData(PipedOutputStream os, int startShard, int endShard);
    public abstract void writeTableData(PipedInputStream is, int startShard, int endShard);
    public abstract String getPrivileges();
    public abstract void setPrivileges(String privInfo);
    public abstract Map<Integer, Integer> getBatches(int numBatch, int dstShardCount, boolean disableShardCopy);
    public void close() {}
    public int getShardCount() {return 0;}
}
