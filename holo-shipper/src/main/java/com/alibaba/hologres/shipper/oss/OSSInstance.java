package com.alibaba.hologres.shipper.oss;

import com.alibaba.hologres.shipper.HoloShipper;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractInstance;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class OSSInstance extends AbstractInstance {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSInstance.class);

    OSS ossClient;
    String bucketName;
    String instancePath;

    public OSSInstance(String endpoint, String accessKeyId, String accessKeySecret, String bucketName, String instancePath) {
        this.ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
        this.bucketName = bucketName;
        this.instancePath = instancePath;
    }

    public List<String> getDBList() {
        List<String> dbList = new ArrayList<String>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
        listObjectsRequest.setDelimiter("/");
        listObjectsRequest.setPrefix(instancePath);
        ObjectListing listing = ossClient.listObjects(listObjectsRequest);
        int n = instancePath.length();
        for (String commonPrefix : listing.getCommonPrefixes()) {
            String dbName = commonPrefix.substring(n, commonPrefix.length()-1);
            dbList.add(dbName);
        }
        return dbList;
    }
    public AbstractDB getDB(String dbName) {
        return new OSSDB(ossClient, bucketName, instancePath, dbName);

    }
    public List<String> getAllRoles() {
        List<String> roleList = new ArrayList<String>();
        String roleFilePath = instancePath+"roles.txt";
        boolean exist = ossClient.doesObjectExist(bucketName,roleFilePath);
        if(!exist) {
            LOGGER.info("No role info stored");
            return roleList;
        }
        OSSObject ossObject = ossClient.getObject(bucketName, roleFilePath);
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()))) {
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                roleList.add(line);
            }
        } catch (IOException e) {
            LOGGER.error("Failed reading role file at "+roleFilePath, e);
        }
        return roleList;
    }
    public void createRoles(List<String> roles, Map<String, Boolean> roleInfo) {
        String roleFilePath = instancePath+"roles.txt";
        String rolesContent = "";
        for(String role : roles)
            rolesContent = rolesContent + role + "\n";
        OSSUtils.writeStringToFile(ossClient, bucketName, roleFilePath, rolesContent);
        String roleInfoPath = instancePath+"roleInfo.txt";
        StringBuilder roleInfoContent = new StringBuilder();
        for(String role : roleInfo.keySet()) {
            roleInfoContent.append(role);
            if(roleInfo.get(role))
                roleInfoContent.append(":user\n");
            else
                roleInfoContent.append(":role\n");
        }
        OSSUtils.writeStringToFile(ossClient, bucketName, roleInfoPath, roleInfoContent.toString());
    }
    public void showVersion() {
        String versionFilePath = instancePath+"version.txt";
        Date date = new Date();
        String versionContent = "";
        versionContent += "=========holo-shipper version==========\n";
        versionContent += "version: " + HoloShipper.VERSION + "\n";
        versionContent += ("date: " + date.toString() + "\n");
        versionContent += "=======================================\n";
        OSSUtils.writeStringToFile(ossClient, bucketName, versionFilePath, versionContent);
    }
    public void createDB(String dbName) {}
    public void close() {
        ossClient.shutdown();
    }

    public Map<String, Boolean> getRoleInfo(List<String> roleList) {
        Map<String, Boolean> roleInfo = new HashMap<>();
        String roleInfoPath = instancePath + "roleInfo.txt";
        boolean exist = ossClient.doesObjectExist(bucketName,roleInfoPath);
        if(!exist) {
            LOGGER.info("No roleInfo file stored");
            return roleInfo;
        }
        OSSObject ossObject = ossClient.getObject(bucketName, roleInfoPath);
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()))) {
            while (true) {
                String line = reader.readLine();
                if (line == null) break;
                String roleName = line.substring(0, line.length()-5);
                roleInfo.put(roleName, line.endsWith(":user"));
            }
        } catch (IOException e) {
            LOGGER.error("Failed reading role file at "+roleInfoPath, e);
        }
        return roleInfo;
    }
}
