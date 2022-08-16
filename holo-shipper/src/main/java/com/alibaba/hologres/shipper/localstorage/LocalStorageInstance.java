package com.alibaba.hologres.shipper.localstorage;

import com.alibaba.hologres.shipper.HoloShipper;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class LocalStorageInstance extends AbstractInstance {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageInstance.class);

    String instancePath;
    public LocalStorageInstance(String instancePath) {
        this.instancePath = instancePath;
    }
    public List<String> getDBList() {
        List<String> dbList = new ArrayList<String>();
        File instance = new File(instancePath);
        File[] files = instance.listFiles();
        for (File f : files){
            if (f.isDirectory()) {
                dbList.add(f.getName());
            }
        }
        return dbList;
    }
    public AbstractDB getDB(String dbName) {
        return new LocalStorageDB(instancePath+'/'+dbName, dbName);
    }
    public List<String> getAllRoles() {
        List<String> roleList = new ArrayList<String>();
        String roleFilePath = instancePath+"/roles.txt";
        File roleFile = new File(roleFilePath);
        if(!roleFile.exists()) {
            LOGGER.info("No role info stored");
            return roleList;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(roleFilePath))) {
            String line;
            while ((line = br.readLine()) != null)
                roleList.add(line);
        } catch(Exception e) {
            LOGGER.error("Failed reading role file at "+roleFilePath, e);
        }
        return roleList;
    }
    public void createRoles(List<String> roles, Map<String, Boolean> roleInfo) {
        String roleFilePath = instancePath+"/roles.txt";
        try {
            File file = new File(roleFilePath);
            file.getParentFile().mkdirs();
            try(FileWriter fw = new FileWriter(file)) {
                for(String roleName:roles) {
                    fw.write(roleName);
                    fw.write("\n");
                }
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+roleFilePath, e);
        }
        String roleInfoPath = instancePath + "/roleInfo.txt";
        try {
            File file = new File(roleInfoPath);
            try(FileWriter fw = new FileWriter(file)) {
                for(String roleName:roleInfo.keySet()) {
                    fw.write(roleName);
                    if(roleInfo.get(roleName))
                        fw.write(":user");
                    else
                        fw.write(":role");
                    fw.write("\n");
                }
            }
        }catch (IOException e) {
            LOGGER.error("Failed writing to "+roleInfoPath, e);
        }
    }
    public void showVersion() {
        File versionFile = new File(instancePath+"/version.txt");
        versionFile.getParentFile().mkdirs();
        Date date = new Date();
        try(FileWriter fw = new FileWriter(versionFile)) {
            fw.write("=========holo-shipper version==========\n");
            fw.write("version: " + HoloShipper.VERSION + "\n");
            fw.write("date: " + date.toString() + "\n");
            fw.write("=======================================\n");
        }catch (IOException e) {
            LOGGER.error("Failed writing version file", e);
        }
    }

    public void createDB(String dbName) {
        File dbDir = new File(instancePath+'/'+dbName);
        dbDir.mkdirs();
    }

    public Map<String, Boolean> getRoleInfo(List<String> roleList) {
        Map<String, Boolean> roleInfo = new HashMap<>();
        String roleInfoPath = instancePath + "/roleInfo.txt";
        File roleFile = new File(roleInfoPath);
        if(!roleFile.exists()) {
            LOGGER.info("No roleInfo file stored");
            return roleInfo;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(roleInfoPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String roleName = line.substring(0, line.length()-5);
                roleInfo.put(roleName, line.endsWith(":user"));
            }
        } catch(Exception e) {
            LOGGER.error("Failed reading roleInfo file at "+roleInfoPath, e);
        }
        return roleInfo;
    }

}
