/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import java.io.Serializable;

/** 分区类. */
public class Partition implements Serializable {
    String schemaName;
    String tableName;
    String parentSchemaName;
    String parentTableName;
    String partitionValue;

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setParentSchemaName(String parentSchemaName) {
        this.parentSchemaName = parentSchemaName;
    }

    public void setParentTableName(String parentTableName) {
        this.parentTableName = parentTableName;
    }

    public void setPartitionValue(String partitionValue) {
        this.partitionValue = partitionValue;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getParentSchemaName() {
        return parentSchemaName;
    }

    public String getParentTableName() {
        return parentTableName;
    }

    public String getPartitionValue() {
        return partitionValue;
    }
}
