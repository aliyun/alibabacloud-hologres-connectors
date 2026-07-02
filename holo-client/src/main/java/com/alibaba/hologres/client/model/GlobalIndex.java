package com.alibaba.hologres.client.model;

import java.io.Serializable;
import java.util.Arrays;

public class GlobalIndex implements Serializable {
    TableName indexName;
    String[] indexKeys;

    public GlobalIndex(TableName indexName, String[] indexKeys) {
        this.indexName = indexName;
        this.indexKeys = indexKeys;
    }

    public TableName getIndexName() {
        return indexName;
    }

    public String[] getIndexKeys() {
        return indexKeys;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(indexName).append(":").append(Arrays.toString(indexKeys)).append("}");
        return sb.toString();
    }
}
