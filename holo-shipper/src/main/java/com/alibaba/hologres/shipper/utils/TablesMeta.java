package com.alibaba.hologres.shipper.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TablesMeta {
    public static final Logger LOGGER = LoggerFactory.getLogger(TablesMeta.class);

    public List<TableInfo> tableInfoList;
    public Map<String, String> ownerInfo;
    public Map<String, List<String>> granteesInfo; //tableName: its privileges grantees
    public Map<String, List<String>> spmInfo; //groupName: users in that group
    public Map<String, List<String>> slpmInfo; //groupName: users in that group

    public static TablesMeta getMetadata(JSONObject shipList, JSONObject blackList, boolean restoreOwner, boolean restorePriv, boolean restoreForeign, boolean restoreView, String metaContent, String dbName) {
        JSONObject dbMeta = JSON.parseObject(metaContent);
        JSONArray allTables = dbMeta.getJSONArray("tableInfo");
        TablesMeta tablesMeta = new TablesMeta();
        List<TableInfo> tableInfoList = new ArrayList<>();
        Set<String> tables = new HashSet<>();
        //找到所有符合shipList的表和他们的父表/子表
        for(int i=0; i<allTables.size(); i++) {
            JSONObject table = allTables.getJSONObject(i);
            String schemaName = table.getString("schemaname");
            String tableName = table.getString("tablename");
            String parentSchema = table.getString("parentschema");
            String parentTable = table.getString("parenttable");
            boolean isForeign = table.getBoolean("foreign");
            boolean isView = table.getBoolean("view");
            if ((!isView || restoreView) && (!isForeign || restoreForeign) && (AbstractDB.filterListContains(shipList, schemaName, tableName) || (parentSchema != null && AbstractDB.filterListContains(shipList, parentSchema, parentTable)))) {
                tables.add(schemaName + '.' + tableName);
                if (parentSchema != null ) {
                    tables.add(parentSchema + '.' + parentTable);
                }
            }
        }
        //根据blackList filter
        if(blackList != null) {
            for(int i=0; i<allTables.size(); i++) {
                JSONObject table = allTables.getJSONObject(i);
                String schemaName = table.getString("schemaname");
                String tableName = table.getString("tablename");
                String parentSchema = table.getString("parentschema");
                String parentTable = table.getString("parenttable");
                if (AbstractDB.filterListContains(blackList, schemaName, tableName) || (parentSchema != null && AbstractDB.filterListContains(blackList, parentSchema, parentTable))) {
                    tables.remove(schemaName + '.' + tableName);
                }
            }
        }
        for(int i=0; i<allTables.size(); i++) {
            JSONObject table = allTables.getJSONObject(i);
            String schemaName = table.getString("schemaname");
            String tableName = table.getString("tablename");
            if(tables.contains(schemaName + '.' + tableName)) {
                TableInfo tableInfo = new TableInfo();
                tableInfo.schemaName = schemaName;
                tableInfo.tableName = tableName;
                tableInfo.isPartitioned = table.getBoolean("partitioned");
                tableInfo.parentSchema = table.getString("parentschema");
                tableInfo.parentTable = table.getString("parenttable");
                tableInfo.isForeign = table.getBoolean("foreign");
                tableInfo.isView = table.getBoolean("view");
                tableInfoList.add(tableInfo);
            }
        }
        tablesMeta.tableInfoList = tableInfoList;
        //get spm and slpm info
        JSONObject spmInfo = dbMeta.getJSONObject("spmInfo");
        JSONObject slpmInfo = dbMeta.getJSONObject("slpmInfo");
        if(spmInfo != null) {
            restoreOwner = false;
            restorePriv = false;
            Map<String, List<String>> spmInfoMap = new HashMap<>();
            for(String groupName : spmInfo.keySet()) {
                List<String> members = jsonToList(spmInfo.getJSONArray(groupName));
                spmInfoMap.put(groupName, members);
            }
            tablesMeta.spmInfo = spmInfoMap;
        } else
            tablesMeta.spmInfo = null;
        if(slpmInfo != null) {
            restoreOwner = false;
            restorePriv = false;
            Map<String, List<String>> slpmInfoMap = new HashMap<>();
            List<String> roleGroups = new ArrayList<>();
            roleGroups.add(String.format("%s.admin", dbName));
            for(String schemaName: shipList.keySet()) {
                roleGroups.add(String.format("%s.%s.developer", dbName, schemaName));
                roleGroups.add(String.format("%s.%s.writer", dbName, schemaName));
                roleGroups.add(String.format("%s.%s.viewer", dbName, schemaName));
            }
            for(String groupName : roleGroups) {
                List<String> members = jsonToList(slpmInfo.getJSONArray(groupName));
                slpmInfoMap.put(groupName, members);
            }
            tablesMeta.slpmInfo = slpmInfoMap;
        } else
            tablesMeta.slpmInfo = null;
        //get owner info
        if(restoreOwner) {
            JSONObject allOwnerInfo = dbMeta.getJSONObject("ownerInfo");
            if(allOwnerInfo != null) {
                Map<String, String> ownerInfo = new HashMap<>();
                for(String table : tables) {
                    String owner = allOwnerInfo.getString(table);
                    if (owner != null)
                        ownerInfo.put(table, owner);
                }
                tablesMeta.ownerInfo = ownerInfo;
            }
            else {
                LOGGER.warn("Owner info was not dumped! will not restore owner for database " + dbName);
                tablesMeta.ownerInfo = null;
            }
        }
        else
            tablesMeta.ownerInfo = null;
        //get privileges info
        if(restorePriv) {
            JSONObject grantees = dbMeta.getJSONObject("granteesInfo");
            if(grantees != null) {
                Map<String, List<String>> granteesInfo = new HashMap<>();
                for(String table : tables) {
                    List<String> tableGrantees = jsonToList(grantees.getJSONArray(table));
                    granteesInfo.put(table, tableGrantees);
                }
                tablesMeta.granteesInfo = granteesInfo;
            }
            else {
                LOGGER.warn("Privileges info was not dumped! will not restore table privileges for database " + dbName);
                tablesMeta.granteesInfo = null;
            }
        }
        else
            tablesMeta.granteesInfo = null;
        LOGGER.info("Finished reading metadata for database " + dbName);
        return tablesMeta;
    }

    public static JSONObject toJSON(TablesMeta tablesMeta) {
        JSONObject dbInfo = new JSONObject();
        JSONArray tableInfoList = new JSONArray();
        for(TableInfo table: tablesMeta.tableInfoList) {
            JSONObject tableInfo = new JSONObject();
            tableInfo.put("schemaname", table.schemaName);
            tableInfo.put("tablename", table.tableName);
            tableInfo.put("partitioned", table.isPartitioned);
            tableInfo.put("parentschema", table.parentSchema);
            tableInfo.put("parenttable", table.parentTable);
            tableInfo.put("foreign", table.isForeign);
            tableInfo.put("view", table.isView);
            tableInfoList.add(tableInfo);
        }
        dbInfo.put("tableInfo", tableInfoList);
        if(tablesMeta.ownerInfo != null) {
            JSONObject owners = new JSONObject();
            for (Map.Entry<String, String> entry : tablesMeta.ownerInfo.entrySet()) {
                if (entry.getValue() != null)
                    owners.put(entry.getKey(), entry.getValue());
            }
            dbInfo.put("ownerInfo", owners);
        }
        if(tablesMeta.granteesInfo != null) {
            JSONObject grantees = mapToJson(tablesMeta.granteesInfo);
            dbInfo.put("granteesInfo", grantees);
        }
        if(tablesMeta.spmInfo != null) {
            JSONObject spm = mapToJson(tablesMeta.spmInfo);
            dbInfo.put("spmInfo", spm);
        }
        if(tablesMeta.slpmInfo != null) {
            JSONObject slpm = mapToJson(tablesMeta.slpmInfo);
            dbInfo.put("slpmInfo", slpm);
        }
        return dbInfo;
    }

    public static List<String> jsonToList(JSONArray json) {
        List<String> list = new ArrayList<>();
        for(int i=0; i<json.size(); i++)
            list.add(json.getString(i));
        return list;
    }

    public static JSONObject mapToJson(Map<String, List<String>> map) {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            json.put(entry.getKey(), JSONArray.parseArray(JSON.toJSONString(entry.getValue())));
        }
        return json;
    }
}
