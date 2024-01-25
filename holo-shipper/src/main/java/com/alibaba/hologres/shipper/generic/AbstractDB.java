package com.alibaba.hologres.shipper.generic;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.shipper.utils.TablesMeta;

import java.util.List;
import java.util.Map;

public abstract class AbstractDB {
    public abstract AbstractTable getTable(String tableName) throws Exception;
    public abstract boolean checkTableExistence(String tableName);
    //check if table tableName already exists
    public abstract void prepareRead();
    public abstract void prepareWrite();
    public abstract Map<String,String> getGUC();
    public abstract void setGUC(Map<String,String> gucMapping);
    public abstract String getExtension();
    public abstract void setExtension(String extInfo);
    public abstract void createSchemas(List<String> schemaList);
    public abstract TablesMeta getMetadata(JSONObject shipList, JSONObject blackList, boolean restoreOwner, boolean restorePriv, boolean restoreForeign, boolean restoreView);
    public abstract void recordMetadata(TablesMeta dbInfo);
    public abstract void restoreSPM(Map<String, List<String>> spmInfo);
    public abstract void restoreSLPM(Map<String, List<String>> slpmInfo);
    public static boolean filterListContains(JSONObject filterList, String schemaName, String tableName) {
        if(!filterList.containsKey(schemaName))
            return false;
        if(filterList.getJSONArray(schemaName).contains("*"))
            return true;
        if(filterList.getJSONArray(schemaName).contains(tableName))
            return true;
        return false;
    }
}