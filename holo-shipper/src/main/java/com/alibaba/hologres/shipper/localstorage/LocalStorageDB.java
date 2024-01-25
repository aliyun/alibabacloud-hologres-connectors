package com.alibaba.hologres.shipper.localstorage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractTable;
import com.alibaba.hologres.shipper.utils.TableInfo;
import com.alibaba.hologres.shipper.utils.TablesMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class LocalStorageDB extends AbstractDB {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageDB.class);
    String dirPath;
    String dbName;

    public LocalStorageDB(String path, String dbName) {
        this.dirPath = path;
        this.dbName = dbName;
    }

    public TablesMeta getMetadata(JSONObject shipList, JSONObject blackList, boolean restoreOwner, boolean restorePriv, boolean restoreForeign, boolean restoreView) {
        LOGGER.info("Starting reading metadata");
        String metaFilePath = dirPath+"/meta.json";
        String metaContent = null;
        try
        {
            metaContent = new String(Files.readAllBytes(Paths.get(metaFilePath)));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed reading meta info from " + metaFilePath, e);
        }
        TablesMeta tablesMeta = TablesMeta.getMetadata(shipList, blackList, restoreOwner, restorePriv,  restoreForeign, restoreView, metaContent, dbName);
        return tablesMeta;
    }

    public void recordMetadata(TablesMeta tablesMeta) {
        JSONObject dbInfo = TablesMeta.toJSON(tablesMeta);
        String metaFilePath = dirPath+"/meta.json";
        try {
            File file = new File(metaFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                fw.write(JSON.toJSONString(dbInfo));
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+metaFilePath, e);
        }
    }

    public AbstractTable getTable(String tableName) {
        AbstractTable table = new LocalStorageTable(dirPath, tableName);
        return table;
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
        String dataFilePath = dirPath + '/' + schemaName + '/' + encodedTableName +'/'+encodedTableName+".sql";
        File dataFile = new File(dataFilePath);
        return dataFile.exists();
    }

    public void prepareRead() {}
    public void prepareWrite() {}

    public Map<String,String> getGUC() {
        String gucFilePath = dirPath+"/guc.properties";
        File gucFile = new File(gucFilePath);
        if(!gucFile.exists()) {
            LOGGER.info("No GUC info stored");
            return null;
        }
        Properties properties = new Properties();
        try(FileReader reader = new FileReader(gucFile)) {
            properties.load(reader);
        } catch (IOException e) {
            LOGGER.error("Failed reading GUC info from "+gucFilePath, e);
        }
        Map<String, String> gucMapping = new HashMap<String, String>((Map) properties);
        return gucMapping;
    }

    public void setGUC(Map<String,String> gucMapping) {
        if(gucMapping == null) {
            LOGGER.info("No GUC info");
            return;
        }
        String gucFilePath = dirPath+"/guc.properties";
        Properties properties = new Properties();
        properties.putAll(gucMapping);
        try {
            File file = new File(gucFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                properties.store(fw, "GUC INFO");
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+gucFilePath, e);
        }
    }
    public String getExtension() {
        String extFilePath = dirPath+"/ext.sql";
        String extInfo = null;
        File extFile = new File(extFilePath);
        if(!extFile.exists()) {
            LOGGER.info("No extension info stored");
            return extInfo;
        }
        try
        {
            extInfo = new String (Files.readAllBytes(Paths.get(extFilePath)));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed reading extension info from "+extFilePath, e);
        }
        return extInfo;
    }
    public void setExtension(String extInfo) {
        if(extInfo == null) {
            LOGGER.info("No extension info");
            return;
        }
        String extFilePath = dirPath+"/ext.sql";
        try {
            File file = new File(extFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                fw.write(extInfo);
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+extFilePath, e);
        }
    }

    public void createSchemas(List<String> schemaList) {
        for(String schema:schemaList) {
            File schemaDir = new File(dirPath+'/'+schema);
            schemaDir.mkdirs();
        }
    }

    public void restoreSPM(Map<String, List<String>> spmInfo) {}
    public void restoreSLPM(Map<String, List<String>> slpmInfo) {}

}
