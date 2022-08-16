package com.alibaba.hologres.shipper;

import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractTable;
import com.alibaba.hologres.shipper.holo.HoloUtils;
import com.alibaba.hologres.shipper.utils.TableInfo;
import com.alibaba.hologres.shipper.utils.TablesMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class HoloDBShipper {
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloDBShipper.class);
    public static final int NUM_BATCH = 5;
    AbstractDB sourceDB;
    AbstractDB sinkDB;
    String dbName;
    String sinkDbName;
    List<String> schemaList;
    List<String> parentTableList;
    List<String> commonTableList;
    List<String> foreignTableList;
    List<String> viewList;
    Map<String, List<String>> spmInfo;
    Map<String, List<String>> slpmInfo;
    Map<String, String> schemaMapping;
    Map<String, String> tgMapping;
    Map<String, String> parentMapping;
    boolean hasToolkit;

    private static final int RETRY_TIME = 3;
    private static final long RETRY_SLEEP_TIME = 1000;
    public HoloDBShipper(String dbName, String sinkDbName) {
        this.dbName = dbName;
        this.sinkDbName = sinkDbName;
        parentTableList = new ArrayList<>();
        commonTableList = new ArrayList<>();
        foreignTableList = new ArrayList<>();
        viewList = new ArrayList<>();
        parentMapping = new HashMap<>();
    }


    public void setSource(AbstractDB sourceDB) {
        this.sourceDB = sourceDB;
    }
    public void setSink(AbstractDB sinkDB) {
        this.sinkDB = sinkDB;
    }
    public List<String> ship(boolean restoreOwner, boolean restoreGUC, boolean restoreExt, boolean restorePriv, boolean restoreData, ExecutorService threadPoolForTable, ExecutorService threadPoolForShard) {
        LOGGER.info("Starting shipping database "+dbName);
        hasToolkit = sourceDB.prepareRead();
        sinkDB.prepareWrite();
        if(restoreExt) {
            String extInfo = sourceDB.getExtension();
            sinkDB.setExtension(extInfo);
        }
        if(restoreGUC) {
            String gucInfo = sourceDB.getGUC();
            sinkDB.setGUC(gucInfo);
        }
        sinkDB.createSchemas(this.schemaList);
        if(restorePriv && spmInfo != null) {
            restoreOwner = false;
            restorePriv = false;
            sinkDB.restoreSPM(spmInfo);
        }
        if(restorePriv && slpmInfo != null) {
            restoreOwner = false;
            restorePriv = false;
            sinkDB.restoreSLPM(slpmInfo);
        }
        List<String> parentFailedList = shipTables(restoreOwner, restorePriv, this.parentTableList, threadPoolForTable, threadPoolForShard, false);
        List<String> commonFailedList = shipTables(restoreOwner, restorePriv, this.commonTableList, threadPoolForTable, threadPoolForShard, restoreData);
        List<String> foreignFailedList = shipTables(false, restorePriv, this.foreignTableList, threadPoolForTable, threadPoolForShard, false);
        List<String> viewFailedList = shipTables(false, restorePriv, this.viewList, threadPoolForTable, threadPoolForShard, false);
        commonFailedList.addAll(parentFailedList);
        commonFailedList.addAll(foreignFailedList);
        commonFailedList.addAll(viewFailedList);

        LOGGER.info("Finished shipping database "+dbName);
        return commonFailedList;
    }

    public List<String> shipTables(boolean restoreOwner, boolean restorePriv, List<String> tablesToShip, ExecutorService threadPoolForTable, ExecutorService threadPoolForShard, boolean shipData) {
        List<String> failedTablesList = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(tablesToShip.size());
        for(String table:tablesToShip) {
            threadPoolForTable.execute(new Runnable() {
                public void run() {
                    try {
                        String sinkTableName = getSinkTableName(table);
                        if (sinkDB.checkTableExistence(sinkTableName)) {
                            LOGGER.warn(String.format("Table %s already exists in %s, skipping", sinkTableName, dbName));
                            return;
                        }
                        for(int i = 0; i < RETRY_TIME; ++i) {
                            try (AbstractTable sourceTable = sourceDB.getTable(table);
                                AbstractTable sinkTable = sinkDB.getTable(sinkTableName))
                            {
                                String tableDDL = sourceTable.getTableDDL(hasToolkit);
                                String rectifiedDDL = rectifyDDL(table, tableDDL, restoreOwner);
                                sinkTable.setTableDDL(rectifiedDDL);
                                if(restorePriv) {
                                    String privInfo = sourceTable.getPrivileges();
                                    privInfo = privInfo.replace(HoloUtils.getTableNameWithQuotes(table), HoloUtils.getTableNameWithQuotes(sinkTableName));
                                    sinkTable.setPrivileges(privInfo);
                                }
                                if(shipData) {
                                    shipTableData(sourceTable, sinkTable, threadPoolForShard, table);
                                }
                                return;
                            } catch(Exception e) {
                                if(i < RETRY_TIME - 1) {
                                    //retry
                                    LOGGER.warn(String.format("Failed shipping table %s, try again", table), e);
                                    Thread.sleep(RETRY_SLEEP_TIME);
                                }
                                else
                                    throw e;
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed shipping table "+table, e);
                        failedTablesList.add(table);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Failed shipping database "+dbName, e);
        }
        return failedTablesList;
    }

    public void shipTableData(AbstractTable sourceTable, AbstractTable sinkTable, ExecutorService threadPoolForShard, String tableName) {
        Map<Integer, Integer> batches = sourceTable.getBatches(NUM_BATCH, sinkTable.getShardCount());
        final CountDownLatch latch = new CountDownLatch(batches.size());
        for(int startShard : batches.keySet()) {
            int endShard = batches.get(startShard);
            threadPoolForShard.execute(new Runnable() {
                public void run() {
                    try {
                        PipedOutputStream os = new PipedOutputStream();
                        PipedInputStream is = new PipedInputStream();
                        is.connect(os);
                        Thread exportThread = new Thread(new Runnable() {
                            public void run() {
                                sourceTable.readTableData(os, startShard, endShard);
                            }
                        });
                        Thread importThread = new Thread(new Runnable() {
                            public void run() {
                                sinkTable.writeTableData(is, startShard, endShard);
                            }
                        });
                        exportThread.start();
                        importThread.start();
                        exportThread.join();
                        importThread.join();
                    } catch (Exception e) {
                        LOGGER.error(String.format("Failed shipping table for table %s from shard %d to %d", tableName, startShard, endShard), e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Failed shipping data for table "+tableName, e);
        }
    }

    public String getSinkTableName(String tableName) {
        String schemaName = tableName.split("\\.",2)[0];
        schemaName = schemaMapping.getOrDefault(schemaName, schemaName);
        return schemaName + "." + tableName.split("\\.",2)[1];
    }

    public String getSinkTableGroupName(String tgName) {
        return tgMapping.get(tgName);
    }

    public String rectifyDDL(String tableName, String tableDDL, boolean restoreOwner){
        String quotedTableName = HoloUtils.getTableNameWithQuotes(tableName);
        tableDDL = tableDDL.replace(tableName, quotedTableName);
        String sinkTableName = HoloUtils.getTableNameWithQuotes(getSinkTableName(tableName));
        tableDDL = tableDDL.replace(quotedTableName, sinkTableName);
        if(parentMapping.containsKey(tableName)) {
            String parentName = parentMapping.get(tableName);
            String quotedParentName = HoloUtils.getTableNameWithQuotes(parentName);
            tableDDL = tableDDL.replace(parentName, quotedParentName);
            String sinkParentName = HoloUtils.getTableNameWithQuotes(getSinkTableName(parentName));
            tableDDL = tableDDL.replace(quotedParentName, sinkParentName);
        }
        String rectifiedDDL = "";
        for(String line: tableDDL.split("\n")) {
            if(line.contains("OWNER TO")) {
                if(restoreOwner) {
                    rectifiedDDL = rectifiedDDL + line + "\n";
                }
            }
            else if(line.startsWith(("-- DEPENDANTS"))) {
                rectifiedDDL = rectifiedDDL+"END;\n";
                break;
            }
            else if (line.startsWith("CALL") && line.contains("'orientation'") && line.contains("''"))
                rectifiedDDL = rectifiedDDL + "-- " + line + "\n";
            else if (line.startsWith("CALL") && line.contains("'table_group'")) { //不对table group进行设置
                int endIndex = line.lastIndexOf("'");
                int startIndex = line.lastIndexOf("'", endIndex-1) + 1;
                String srcTG = line.substring(startIndex, endIndex);
                String dstTG = getSinkTableGroupName(srcTG);
                if (dstTG != null)
                    rectifiedDDL = rectifiedDDL + line.replaceAll(srcTG, dstTG) + "\n";
                else
                    rectifiedDDL = rectifiedDDL + "-- " + line + "\n";
            }
            else if (line.startsWith("CALL") && line.contains("'colocate_with'")) //0.8时期colocate_with用法会引起引擎不兼容等问题
                rectifiedDDL = rectifiedDDL + "-- " + line + "\n";
            else
                rectifiedDDL = rectifiedDDL+line+"\n";
        }
        return rectifiedDDL;
    }

    public void setTableListMeta(TablesMeta tableListMeta, Map<String, String> schemaMapping, Map<String, String> tgMapping) {
        this.schemaMapping = schemaMapping;
        this.tgMapping = tgMapping;
        TablesMeta newMeta = new TablesMeta();
        Set<String> schemaSet = new HashSet<>();
        List<TableInfo> newTableInfoList = new ArrayList<>();
        for(TableInfo tableInfo: tableListMeta.tableInfoList) {
            Boolean isPartitioned = tableInfo.isPartitioned;
            Boolean isForeign = tableInfo.isForeign;
            Boolean isView = tableInfo.isView;
            String schemaName = tableInfo.schemaName;
            schemaSet.add(schemaMapping.getOrDefault(schemaName, schemaName));
            String tableName = tableInfo.schemaName + "." + tableInfo.tableName;
            if(isPartitioned)
                parentTableList.add(tableName);
            else if (isForeign)
                foreignTableList.add(tableName);
            else if (isView)
                viewList.add(tableName);
            else
                commonTableList.add(tableName);
            if(tableInfo.parentSchema != null) {
                String parentName = tableInfo.parentSchema + "." + tableInfo.parentTable;
                parentMapping.put(tableName, parentName);
            }
            TableInfo newInfo = new TableInfo();
            newInfo.schemaName = schemaMapping.getOrDefault(schemaName, schemaName);
            newInfo.tableName = tableInfo.tableName;
            newInfo.isPartitioned = tableInfo.isPartitioned;
            newInfo.parentSchema = schemaMapping.getOrDefault(tableInfo.parentSchema, tableInfo.parentSchema);
            newInfo.parentTable = tableInfo.parentTable;
            newInfo.isForeign = tableInfo.isForeign;
            newInfo.isView = tableInfo.isView;
            newTableInfoList.add(newInfo);
        }
        newMeta.tableInfoList = newTableInfoList;
        this.schemaList = new ArrayList<>(schemaSet);
        if(tableListMeta.ownerInfo != null) {
            Map<String, String> newOwnerInfo = new HashMap<>();
            for (String table : tableListMeta.ownerInfo.keySet())
                newOwnerInfo.put(getSinkTableName(table), tableListMeta.ownerInfo.get(table));
            newMeta.ownerInfo = newOwnerInfo;
        }
        else
            newMeta.ownerInfo = null;
        if(tableListMeta.granteesInfo != null) {
            Map<String, List<String>> newGranteesInfo = new HashMap<>();
            for (String table : tableListMeta.granteesInfo.keySet())
                newGranteesInfo.put(getSinkTableName(table), tableListMeta.granteesInfo.get(table));
            newMeta.granteesInfo = newGranteesInfo;
        }
        else
            newMeta.granteesInfo = null;
        if(tableListMeta.spmInfo == null || dbName.equals(sinkDbName))
            newMeta.spmInfo = tableListMeta.spmInfo;
        else {
            Map<String, List<String>> newSpmInfo = new HashMap<>();
            String[] suffixes = {"_admin", "_developer", "_writer", "_viewer"};
            for (String suffix : suffixes)
                newSpmInfo.put(sinkDbName+suffix, tableListMeta.spmInfo.get(dbName+suffix));
            newMeta.spmInfo = newSpmInfo;
        }
        if(tableListMeta.slpmInfo != null) {
            Map<String, List<String>> newSlpmInfo = new HashMap<>();
            for(String groupName : tableListMeta.slpmInfo.keySet()) {
                if(groupName.equals(dbName + ".admin"))
                    newSlpmInfo.put(sinkDbName + ".admin", tableListMeta.slpmInfo.get(groupName));
                else {
                    String schema = groupName.split("\\.")[1];
                    String newSchema = schemaMapping.getOrDefault(schema, schema);
                    String newGroupName = sinkDbName + "." + newSchema + "." + groupName.split("\\.")[2];
                    if(!newSlpmInfo.containsKey(newGroupName))
                        newSlpmInfo.put(newGroupName, new ArrayList<>());
                    newSlpmInfo.get(newGroupName).addAll(tableListMeta.slpmInfo.get(groupName));
                }
            }
            newMeta.slpmInfo = newSlpmInfo;
        }
        else
            newMeta.slpmInfo = null;
        sinkDB.recordMetadata(newMeta);
        this.spmInfo = newMeta.spmInfo;
        this.slpmInfo = newMeta.slpmInfo;
    }

}
