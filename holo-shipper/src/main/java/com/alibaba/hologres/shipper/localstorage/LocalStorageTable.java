package com.alibaba.hologres.shipper.localstorage;

import com.alibaba.hologres.shipper.generic.AbstractTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;

public class LocalStorageTable extends AbstractTable {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageTable.class);

    String tablePath;
    String tableName;
    String schemaName;
    String pureTableName;
    String encodedTableName;


    public LocalStorageTable(String DBPath, String tableName) {
        this.tableName = tableName;
        this.schemaName = tableName.split("\\.",2)[0];
        this.pureTableName = tableName.split("\\.",2)[1];
        try {
            this.encodedTableName = URLEncoder.encode(this.pureTableName, "UTF-8");
        }catch(UnsupportedEncodingException e) {
            LOGGER.error("Unsupported encoding", e);
        }
        this.tablePath = DBPath + '/' + schemaName + '/' + encodedTableName;
    }

    public String getTableDDL() {
        String DDLFilePath = tablePath+'/'+encodedTableName+".sql";
        String DDLInfo = null;
        try
        {
            DDLInfo = new String (Files.readAllBytes(Paths.get(DDLFilePath)));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed reading DDL info for table "+tableName, e);
        }
        return DDLInfo;
    }

    public void setTableDDL(String DDLInfo) throws Exception{
        String DDLFilePath = tablePath+'/'+encodedTableName+".sql";
        try {
            File file = new File(DDLFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                fw.write(DDLInfo);
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+DDLFilePath, e);
            throw e;
        }

    }


    public String getPrivileges() {
        String privFilePath = tablePath+'/'+encodedTableName+"_priv.sql";
        String privInfo = null;
        File privFile = new File(privFilePath);
        if(!privFile.exists()) {
            LOGGER.info("No privileges info stored for " + tableName);
            return privInfo;
        }
        try
        {
            privInfo = new String (Files.readAllBytes(Paths.get(privFilePath)));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed reading privileges info from " + privFilePath, e);
        }
        return privInfo;
    }

    public void setPrivileges(String privInfo) {
        if(privInfo == null) {
            LOGGER.info("No privileges info for table " + tableName);
            return;
        }
        String privFilePath = tablePath+'/'+encodedTableName+"_priv.sql";
        try {
            File file = new File(privFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                fw.write(privInfo);
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to " + privFilePath, e);
        }
    }

    public void readTableData(PipedOutputStream os, int startShard, int endShard) {
        String dataFileSuffix = String.format("%d-%d.data", startShard, endShard);
        if(startShard == -1)
            dataFileSuffix = ".data";
        File table = new File(tablePath);
        File[] files = table.listFiles();
        try{
            for (File f : files){
                String fileName = f.getName();
                if(fileName.endsWith(dataFileSuffix)) {
                    try(FileInputStream fs = new FileInputStream(f)) {
                        int data;
                        while((data = fs.read()) != -1)
                            os.write(data);
                    }
                }
            }
            LOGGER.info(String.format("Read table %s done", tableName));
        }catch(Exception e) {
            LOGGER.error("Failed reading data from table " + tableName, e);
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                LOGGER.error("Failed closing PipedOutputStream", e);
            }
        }
    }
    public void writeTableData(PipedInputStream is, int startShard, int endShard) {
        String dataFilePath = String.format("%s/%d-%d.data", tablePath, startShard, endShard);
        if(startShard == -1)
            dataFilePath = String.format("%s/all.data", tablePath);
        File file = new File(dataFilePath);
        try(FileOutputStream fs = new FileOutputStream(file)) {
            int data;
            while ((data = is.read()) != -1)
                fs.write(data);
            LOGGER.info(String.format("Write table %s from shard %d to %d done", tableName, startShard, endShard));
        } catch(Exception e) {
            LOGGER.error("Failed writing data to table " + tableName, e);
        } finally {
            try {
                is.close();
            }catch(IOException e) {
                LOGGER.error("Failed closing PipedInputStream", e);
            }
        }
    }

    public Map<Integer, Integer> getBatches(int numBatch, int dstShardCount, boolean disableShardCopy) {
        Map<Integer, Integer> batches = new HashMap<>();
        Map<Integer, Integer> defaultBatches = new HashMap<>();
        defaultBatches.put(-1, -1);
        int shardCount = 0;
        File table = new File(tablePath);
        File[] files = table.listFiles();
        for (File f : files){
            String fileName = f.getName();
            if(fileName.endsWith(".data")) {
                String shardRange = fileName.split("\\.")[0];
                if(shardRange.equals("all")) {
                    return defaultBatches;
                }
                int startShard = Integer.parseInt(shardRange.split("-")[0]);
                int endShard = Integer.parseInt(shardRange.split("-")[1]);
                shardCount = max(shardCount, endShard);
                batches.put(startShard, endShard);
            }
        }
        if(shardCount == dstShardCount || dstShardCount == 0 || !disableShardCopy)
            return batches;
        return defaultBatches;
    }
}
