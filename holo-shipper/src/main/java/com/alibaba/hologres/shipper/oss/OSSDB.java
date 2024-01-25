package com.alibaba.hologres.shipper.oss;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractTable;
import com.alibaba.hologres.shipper.utils.TablesMeta;
import com.aliyun.oss.OSS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OSSDB extends AbstractDB {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSDB.class);

    OSS ossClient;
    String bucketName;
    String dbPath;
    String dbName;

    public OSSDB(OSS ossClient, String bucketName, String instancePath, String dbName) {
        this.ossClient = ossClient;
        this.bucketName = bucketName;
        this.dbPath = instancePath + dbName + "/";
        this.dbName = dbName;
    }

    public TablesMeta getMetadata(JSONObject shipList, JSONObject blackList, boolean restoreOwner, boolean restorePriv, boolean restoreForeign, boolean restoreView) {
        LOGGER.info("Starting reading metadata");
        String metaFilePath = dbPath + "meta.json";
        String metaContent = OSSUtils.readWholeFile(ossClient, bucketName, metaFilePath);
        TablesMeta tablesMeta = TablesMeta.getMetadata(shipList, blackList, restoreOwner, restorePriv, restoreForeign, restoreView, metaContent, dbName);
        return tablesMeta;
    }

    public void recordMetadata(TablesMeta tablesMeta) {
        JSONObject dbInfo = TablesMeta.toJSON(tablesMeta);
        String metaFilePath = dbPath + "meta.json";
        OSSUtils.writeStringToFile(ossClient, bucketName, metaFilePath, JSON.toJSONString(dbInfo));
    }

    public AbstractTable getTable(String tableName) {
        return new OSSTable(ossClient, bucketName, dbPath, tableName);
    }

    public boolean checkTableExistence(String tableName) {
        String schemaName = tableName.split("\\.",2)[0];
        String pureTableName = tableName.split("\\.",2)[1];
        String encodedTableName = null;
        try {
            encodedTableName = URLEncoder.encode(pureTableName, "UTF-8");
        }catch(UnsupportedEncodingException e) {
            LOGGER.error("Unsupported encoding", e);
        }
        String dataFilePath = dbPath + schemaName + '/' + encodedTableName +'/'+encodedTableName+".sql";
        boolean found = ossClient.doesObjectExist(bucketName, dataFilePath);
        return found;
    }

    public void prepareRead() {}
    public void prepareWrite() {}

    public Map<String,String> getGUC() {
        String gucFilePath = dbPath + "guc.properties";
        if(!ossClient.doesObjectExist(bucketName, gucFilePath)) {
            LOGGER.info("No GUC info stored");
            return null;
        }
        Properties properties = OSSUtils.loadProperties(ossClient, bucketName, gucFilePath);
        Map<String, String> gucMapping = new HashMap<String, String>((Map) properties);
        return gucMapping;
    }

    public void setGUC(Map<String,String> gucMapping) {
        if(gucMapping == null) {
            LOGGER.info("No GUC info");
            return;
        }
        String gucFilePath = dbPath + "guc.properties";
        Properties properties = new Properties();
        properties.putAll(gucMapping);
        OSSUtils.storeProperties(ossClient, bucketName, gucFilePath, properties);
    }

    public String getExtension() {
        String extFilePath = dbPath + "ext.sql";
        String extInfo = null;
        if(!ossClient.doesObjectExist(bucketName, extFilePath)) {
            LOGGER.info("No extension info stored");
            return extInfo;
        }
        extInfo = OSSUtils.readWholeFile(ossClient, bucketName, extFilePath);
        return extInfo;
    }
    public void setExtension(String extInfo) {
        if(extInfo == null) {
            LOGGER.info("No extension info");
            return;
        }
        String extFilePath = dbPath + "ext.sql";
        OSSUtils.writeStringToFile(ossClient, bucketName, extFilePath, extInfo);
    }

    public void createSchemas(List<String> schemaList) {}
    public void restoreSPM(Map<String, List<String>> spmInfo) {}
    public void restoreSLPM(Map<String, List<String>> slpmInfo) {}


}
