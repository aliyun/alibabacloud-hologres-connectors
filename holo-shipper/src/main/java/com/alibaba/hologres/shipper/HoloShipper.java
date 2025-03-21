package com.alibaba.hologres.shipper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractInstance;
import com.alibaba.hologres.shipper.holo.HoloInstance;
import com.alibaba.hologres.shipper.localstorage.LocalStorageInstance;
import com.alibaba.hologres.shipper.oss.OSSInstance;
import com.alibaba.hologres.shipper.utils.ProcessBar;
import com.alibaba.hologres.shipper.utils.TablesMeta;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HoloShipper {
    public static final String VERSION = "1.2.3";
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloShipper.class);
    AbstractInstance source;
    AbstractInstance sink;
    int MAX_NUM_THREAD_DB = 2;
    int MAX_NUM_THREAD_TABLE = 10;
    int MAX_NUM_THREAD_SHARD = 10;

    boolean restoreOwner;
    boolean restoreAllRoles;
    boolean restoreGUC;
    boolean restoreExt;
    boolean restorePriv;
    boolean restoreData;
    boolean restoreForeign;
    boolean restoreView;
    boolean allowSinkTableExists;
    boolean disableShardCopy;
    String shipListPath;

    List<String> dbsToShip;
    List<String> roleList;
    Map<String, TablesMeta> dbsMeta;
    Map<String, Map<String, String>> schemaMappings;
    Map<String, Map<String, String>> tgMappings;
    Map<String, String> sinkDbNames;
    int shipTableCount = 0;


    public HoloShipper() {
        dbsToShip = new ArrayList<>();
        dbsMeta = new HashMap<>();
        schemaMappings = new HashMap<>();
        tgMappings = new HashMap<>();
        sinkDbNames = new HashMap<>();
    }

    public void getUserRequirements(String[] args) throws ParseException{
        //get user requirements about source, sink, tables to ship, restore owner,schema,guc... or not
        restoreOwner = true;
        restoreAllRoles = true;
        restoreGUC = true;
        restoreExt = true;
        restorePriv = true;
        restoreData = true;
        restoreForeign = true;
        restoreView = true;
        allowSinkTableExists = false;
        disableShardCopy = false;

        Options options = new Options();
        options.addOption(Option.builder("s").hasArgs().required().build());
        options.addOption(Option.builder("d").hasArgs().required().build());
        options.addOption(Option.builder("l").hasArg().required().build());
        options.addOption(Option.builder().longOpt("max-task-num").hasArg().optionalArg(true).build());
        options.addOption(Option.builder().longOpt("no-owner").build());
        options.addOption(Option.builder().longOpt("no-all-roles").build());
        options.addOption(Option.builder().longOpt("no-guc").build());
        options.addOption(Option.builder().longOpt("no-ext").build());
        options.addOption(Option.builder().longOpt("no-priv").build());
        options.addOption(Option.builder().longOpt("no-data").build());
        options.addOption(Option.builder().longOpt("no-foreign").build());
        options.addOption(Option.builder().longOpt("no-view").build());
        options.addOption(Option.builder().longOpt("allow-table-exists").build());
        options.addOption(Option.builder().longOpt("disable-shard-copy").build());
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String[] srcInfo = commandLine.getOptionValues("s");
        LOGGER.info("Source info:");
        this.setSource(parseInstannce(srcInfo));
        String[] dstInfo = commandLine.getOptionValues("d");
        LOGGER.info("Destination info:");
        this.setSink(parseInstannce(dstInfo));
        this.shipListPath = commandLine.getOptionValue("l");
        if (commandLine.getOptionValue("max-task-num") != null) {
            Integer threadNum = Integer.parseInt(commandLine.getOptionValue("max-task-num"));
            LOGGER.info("user config max-task-num: " + threadNum);
            this.MAX_NUM_THREAD_TABLE = Math.min(this.MAX_NUM_THREAD_TABLE, threadNum);
            this.MAX_NUM_THREAD_SHARD = Math.min(this.MAX_NUM_THREAD_SHARD, threadNum);
        }
        if (commandLine.hasOption("disable-shard-copy")) {
            this.disableShardCopy = true;
            LOGGER.info("Disable shard copy");
        }
        if (commandLine.hasOption("no-owner"))
        {
            LOGGER.info("Do not ship owner");
            this.restoreOwner = false;
        }
        if (commandLine.hasOption("no-all-roles"))
        {
            LOGGER.info("Do not ship all roles");
            this.restoreAllRoles = false;
        }
        if (commandLine.hasOption("no-guc"))
        {
            LOGGER.info("Do not ship guc parameters");
            this.restoreGUC = false;
        }
        if (commandLine.hasOption("no-ext"))
        {
            LOGGER.info("Do not ship extensions");
            this.restoreExt = false;
        }
        if (commandLine.hasOption("no-priv"))
        {
            LOGGER.info("Do not ship privileges");
            this.restorePriv = false;
        }
        if (commandLine.hasOption("no-data"))
        {
            LOGGER.info("Do not ship data");
            this.restoreData = false;
        }
        if (commandLine.hasOption("no-foreign"))
        {
            LOGGER.info("Do not ship foreign tables");
            this.restoreForeign = false;
        }
        if (commandLine.hasOption("no-view"))
        {
            LOGGER.info("Do not ship view");
            this.restoreView = false;
        }
        if (commandLine.hasOption("allow-table-exists"))
        {
            LOGGER.info("Allow table exists");
            this.allowSinkTableExists = true;
        }
    }

    public void getTableAndRoleLists() throws Exception{
        //find all tables need to ship and their owners
        JSONArray shipListJson = null;
        try
        {
            String shipListContent = new String(Files.readAllBytes(Paths.get(this.shipListPath)));
            shipListJson = JSON.parseArray(shipListContent);
        }
        catch (IOException e)
        {
            LOGGER.error("Failed reading ship list info from " + shipListPath, e);
            throw e;
        }
        List<String> dbList = source.getDBList();
        Set<String> roleSet = new HashSet<String>();
        for(int i=0; i<shipListJson.size(); i++) {
            JSONObject dbShipList = shipListJson.getJSONObject(i);
            String dbName = dbShipList.getString("dbName");
            if(dbName == null)
                throw new Exception("Invalid ship list json format, no database name");
            if(!dbList.contains(dbName)) {
                LOGGER.warn(String.format("Database %s does not exist, skipping", dbName));
                continue;
            }
            String sinkDb = dbShipList.getString("sinkDB");
            if(sinkDb == null) sinkDb = dbName;
            sinkDbNames.put(dbName, sinkDb);
            JSONObject shipList = dbShipList.getJSONObject("shipList");
            if(shipList == null)
                throw new Exception("Invalid ship list json format, no shipList");
            JSONObject blackList = dbShipList.getJSONObject("blackList");
            dbsToShip.add(dbName);
            AbstractDB db = source.getDB(dbName);
            TablesMeta tableListMeta = db.getMetadata(shipList, blackList, restoreOwner, restorePriv, restoreForeign, restoreView);
            Map<String, String> owners = tableListMeta.ownerInfo;
            if(restoreOwner && owners != null) {
                roleSet.addAll(owners.values());
            }
            Map<String, List<String>> grantees = tableListMeta.granteesInfo;
            if(restorePriv && grantees != null){
                for(List<String> tableGrantees: grantees.values())
                    roleSet.addAll(tableGrantees);
            }
            Map<String, List<String>> spm = tableListMeta.spmInfo;
            if(restorePriv && spm != null){
                for(List<String> spmUsers: spm.values())
                    roleSet.addAll(spmUsers);
            }
            Map<String, List<String>> slpm = tableListMeta.slpmInfo;
            if(restorePriv && slpm != null){
                for(List<String> slpmUsers: slpm.values())
                    roleSet.addAll(slpmUsers);
            }
            dbsMeta.put(dbName, tableListMeta);
            shipTableCount+= tableListMeta.tableInfoList.size();
            JSONObject schemaMappingJson = dbShipList.getJSONObject("schemaMapping");
            Map<String, String> schemaMapping = new HashMap<>();
            if(schemaMappingJson != null) {
                for(String oldSchema : schemaMappingJson.keySet())
                    schemaMapping.put(oldSchema, schemaMappingJson.getString(oldSchema));
            }
            schemaMappings.put(dbName, schemaMapping);

            JSONObject tgMappingJson = dbShipList.getJSONObject("tgMapping");
            Map<String, String> tgMapping = new HashMap<>();
            if(tgMappingJson != null) {
                for (String srcTG : tgMappingJson.keySet())
                    tgMapping.put(srcTG, tgMappingJson.getString(srcTG));
            }
            tgMappings.put(dbName, tgMapping);
        }
        roleList = new ArrayList<>(roleSet);
    }

    public void setSource(AbstractInstance source) {
        this.source = source;
    }
    public void setSink(AbstractInstance sink) {
        this.sink = sink;
    }

    public void ship() {
        LOGGER.info("Starting shipping");
        sink.showVersion();
        try {
            getTableAndRoleLists();
        } catch (Exception e) {
            LOGGER.error("Failed getting table lists to ship", e);
            return;
        }
        Set<String> allRolesSet = new HashSet<>();
        if(restoreAllRoles) {
            allRolesSet.addAll(source.getAllRoles());
        }
        if(restoreOwner || restorePriv) {
            allRolesSet.addAll(roleList);
        }
        List<String> allRolesList = new ArrayList<>(allRolesSet);
        sink.createRoles(allRolesList, source.getRoleInfo(allRolesList));
        Map<String,List<String>> failedTables = new HashMap<String, List<String>>();
        final CountDownLatch latch = new CountDownLatch(dbsToShip.size());
        ExecutorService threadPoolForDB =  Executors.newFixedThreadPool(MAX_NUM_THREAD_DB);
        ExecutorService threadPoolForTable =  Executors.newFixedThreadPool(MAX_NUM_THREAD_TABLE);
        ExecutorService threadPoolForShard =  Executors.newFixedThreadPool(MAX_NUM_THREAD_SHARD);
        ProcessBar processBar = new ProcessBar(shipTableCount);
        for(String db:dbsToShip) {
            String sinkDBName = sinkDbNames.get(db);
            threadPoolForDB.execute(new Runnable() {
                public void run() {
                    try {
                        sink.createDB(sinkDBName);
                        AbstractDB sourceDB = source.getDB(db);
                        AbstractDB sinkDB = sink.getDB(sinkDBName);
                        HoloDBShipper dbShipper = new HoloDBShipper(db, sinkDBName, processBar);
                        dbShipper.setSource(sourceDB);
                        dbShipper.setSink(sinkDB);
                        dbShipper.setTableListMeta(dbsMeta.get(db), schemaMappings.get(db), tgMappings.get(db));
                        dbShipper.setAllowSinkTableExists(allowSinkTableExists);
                        dbShipper.setDisableShardCopy(disableShardCopy);
                        List<String> failedTablesList = dbShipper.ship(restoreOwner,restoreGUC,restoreExt,restorePriv,restoreData,threadPoolForTable,threadPoolForShard);
                        failedTables.put(db, failedTablesList);
                    } catch (Exception e) {
                        LOGGER.error("Failed shipping database "+db, e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            while (!latch.await(5, TimeUnit.SECONDS)) {
                processBar.print();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Shipping failed", e);
        } finally {
            threadPoolForDB.shutdownNow();
            threadPoolForTable.shutdownNow();
            threadPoolForShard.shutdownNow();
            source.close();
            sink.close();
        }

        LOGGER.info("Finished shipping");
        processBar.print();
        informFailedTables(failedTables);
    }

    public void informFailedTables(Map<String,List<String>> failedTables) {
        int count = 0;
        String info = "";
        for (Map.Entry<String,List<String>> entry : failedTables.entrySet()) {
            List<String> failedTablesList = entry.getValue();
            if(!failedTablesList.isEmpty()) {
                count += failedTablesList.size();
                info = info + entry.getKey() + ":\n";
                for(String table : failedTablesList)
                    info = info + table + "\n";
            }
        }
        info = String.format("%d tables failed in shipping\n", count) + info;
        if(count > 0)
            LOGGER.warn(info);
        else
            LOGGER.info("All table shipping finished");

    }

    public HoloInstance parseHoloInstance(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("h").hasArg().required().build());
        options.addOption(Option.builder("p").hasArg().required().build());
        options.addOption(Option.builder("u").hasArg().required().build());
        options.addOption(Option.builder("w").hasArg().required().build());
        CommandLineParser parser = new DefaultParser();

        CommandLine commandLine = parser.parse(options, args);
        LOGGER.info("Holo instance:");
        String ip = commandLine.getOptionValue("h");
        String port = commandLine.getOptionValue("p");
        String username = commandLine.getOptionValue("u");
        String password = commandLine.getOptionValue("w");
        LOGGER.info(String.format("ip: %s port: %s username: %s", ip, port, username));
        return new HoloInstance(ip, port, username, password);
    }

    public OSSInstance parseOSSInstance(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("h").hasArg().required().build());
        options.addOption(Option.builder("u").hasArg().required().build());
        options.addOption(Option.builder("w").hasArg().required().build());
        options.addOption(Option.builder("b").hasArg().required().build());
        options.addOption(Option.builder("p").hasArg().required().build());
        CommandLineParser parser = new DefaultParser();

        CommandLine commandLine = parser.parse(options, args);
        LOGGER.info("OSS instance:");
        String endpoint = commandLine.getOptionValue("h");
        String accessKeyId = commandLine.getOptionValue("u");
        String accessKeySecret = commandLine.getOptionValue("w");
        String bucketName = commandLine.getOptionValue("b");
        String instancePath = commandLine.getOptionValue("p");
        if(!instancePath.endsWith("/"))
            instancePath += "/";
        LOGGER.info(String.format("endpoint: %s bucketName: %s instancePath: %s", endpoint, bucketName, instancePath));
        return new OSSInstance(endpoint, accessKeyId, accessKeySecret, bucketName, instancePath);
    }

    public AbstractInstance parseInstannce(String[] args) throws ParseException{
        if(args[0].equals("holo")) {
            return parseHoloInstance(args);
        }
        else if(args[0].equals("oss")) {
            return parseOSSInstance(args);
        }
        else if(args.length == 1) {
            LOGGER.info("Local Storage instance:");
            String path = args[0];
            LOGGER.info("path: " + path);
            return new LocalStorageInstance(path);
        }
        else{
            throw new ParseException("Invalid instance format!");
        }
    }


    public static void main(String [] args) {
        HoloShipper shipper = new HoloShipper();
        try {
            shipper.getUserRequirements(args);
            shipper.ship();
        } catch(ParseException e) {
            LOGGER.error("Failed parsing arguements: " + e.getMessage());
        }
    }
}
