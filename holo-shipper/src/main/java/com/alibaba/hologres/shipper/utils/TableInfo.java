package com.alibaba.hologres.shipper.utils;

public class TableInfo {
    public String schemaName;
    public String tableName;
    public Boolean isPartitioned;
    public String parentSchema;
    public String parentTable;
    public Boolean isForeign;
    public Boolean isView;
}
