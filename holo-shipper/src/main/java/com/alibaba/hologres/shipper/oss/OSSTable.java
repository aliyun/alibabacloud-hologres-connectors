package com.alibaba.hologres.shipper.oss;

import com.alibaba.hologres.shipper.generic.AbstractTable;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

public class OSSTable extends AbstractTable {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSTable.class);

    OSS ossClient;
    String bucketName;
    String tablePath;
    String tableName;
    String schemaName;
    String pureTableName;
    String encodedTableName;

    public OSSTable(OSS ossClient, String bucketName, String dbPath, String tableName) {
        this.ossClient = ossClient;
        this.bucketName = bucketName;
        this.tableName = tableName;
        this.schemaName = tableName.split("\\.",2)[0];
        this.pureTableName = tableName.split("\\.",2)[1];
        try {
            this.encodedTableName = URLEncoder.encode(this.pureTableName, "UTF-8");
        }catch(UnsupportedEncodingException e) {
            LOGGER.error("Unsupported encoding", e);
        }
        this.tablePath = dbPath + schemaName + '/' + encodedTableName + '/';
    }

    public String getTableDDL() {
        String DDLFilePath = tablePath + encodedTableName + ".sql";
        String DDLInfo = OSSUtils.readWholeFile(ossClient, bucketName, DDLFilePath);
        return DDLInfo;
    }

    public void setTableDDL(String DDLInfo) throws Exception{
        String DDLFilePath = tablePath + encodedTableName + ".sql";
        OSSUtils.writeStringToFile(ossClient, bucketName, DDLFilePath, DDLInfo);
    }

    public String getPrivileges() {
        String privFilePath = tablePath + encodedTableName + "_priv.sql";
        String privInfo = null;
        if(!ossClient.doesObjectExist(bucketName, privFilePath)) {
            LOGGER.info("No privileges info stored for " + tableName);
            return privInfo;
        }
        privInfo = OSSUtils.readWholeFile(ossClient, bucketName, privFilePath);
        return privInfo;
    }

    public void setPrivileges(String privInfo) {
        if(privInfo == null) {
            LOGGER.info("No privileges info for table " + tableName);
            return;
        }
        String privFilePath = tablePath + encodedTableName + "_priv.sql";
        OSSUtils.writeStringToFile(ossClient, bucketName, privFilePath, privInfo);
    }

    public void readTableData(PipedOutputStream os, int startShard, int endShard) {
        String dataFilePathSuffix = String.format("%d-%d.data", startShard, endShard);
        if(startShard == -1)
            dataFilePathSuffix = ".data";
        List<String> dataFiles = new ArrayList<>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
        listObjectsRequest.setPrefix(tablePath);
        ObjectListing listing = ossClient.listObjects(listObjectsRequest);
        for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
            String dataFilePath = objectSummary.getKey();
            if(dataFilePath.endsWith(dataFilePathSuffix)) {
                dataFiles.add(dataFilePath);
            }
        }
        try {
            for(String dataFilePath : dataFiles) {
                OSSObject ossObject = ossClient.getObject(bucketName, dataFilePath);
                try (InputStream fs = ossObject.getObjectContent()) {
                    int data;
                    while ((data = fs.read()) != -1)
                        os.write(data);
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
        String dataFilePath = String.format("%s%d-%d.data", tablePath, startShard, endShard);
        if(startShard == -1)
            dataFilePath = String.format("%sall.data", tablePath);
        try{
            ossClient.putObject(bucketName, dataFilePath, is);
            LOGGER.info(String.format("Write table %s done", tableName));
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
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
        listObjectsRequest.setPrefix(tablePath);
        ObjectListing listing = ossClient.listObjects(listObjectsRequest);
        for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
            String dataFilePath = objectSummary.getKey();
            if(dataFilePath.endsWith(".data")) {
                String shardRange = dataFilePath.substring(tablePath.length()).split("\\.")[0];
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
