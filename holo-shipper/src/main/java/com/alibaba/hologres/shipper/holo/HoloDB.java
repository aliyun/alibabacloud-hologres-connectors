package com.alibaba.hologres.shipper.holo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.org.postgresql.util.HoloVersion;
import com.alibaba.hologres.org.postgresql.util.MetaUtil;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractTable;

import java.sql.*;
import java.util.*;

import com.alibaba.hologres.shipper.utils.TableInfo;
import com.alibaba.hologres.shipper.utils.TablesMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoloDB extends AbstractDB {
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloDB.class);

    String jdbcUrl;
    String user;
    String password;
    String dbName;
    HoloVersion version;

    public HoloDB(String jdbcUrl, String user, String password, String dbName){
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.dbName = dbName;
    }

    public TablesMeta getMetadata(JSONObject shipList, JSONObject blackList, boolean restoreOwner, boolean restorePriv, boolean restoreForeign, boolean restoreView) {
        LOGGER.info("Starting getting metadata from " + jdbcUrl);
        TablesMeta tablesMeta = new TablesMeta();
        List<TableInfo> tableInfoList = new ArrayList<>();
        String searchTablesSql = "SELECT c.relname AS tablename, n.nspname AS schemaname, p.partrelid AS partrelid, cp.relname AS parenttable, np.nspname AS parentschema\n" +
                "  FROM pg_class c LEFT JOIN pg_namespace n ON c.relnamespace = n.oid \n" +
                "                  LEFT JOIN pg_partitioned_table p ON c.oid = p.partrelid \n" +
                "                  LEFT JOIN pg_inherits i ON c.oid = i.inhrelid \n" +
                "                  LEFT JOIN pg_class cp ON i.inhparent = cp.oid \n" +
                "                  LEFT JOIN pg_namespace np ON cp.relnamespace = np.oid \n"+
                "                  WHERE c.relkind IN ('r', 'p')";
        String searchForeignTablesSql = "SELECT c.relname AS tablename, n.nspname AS schemaname\n" +
                "FROM pg_class c LEFT JOIN pg_namespace n ON c.relnamespace = n.oid \n" +
                "WHERE c.relkind = 'f'";
        String searchViewsSql = "SELECT c.relname AS tablename, n.nspname AS schemaname\n" +
                "FROM pg_class c LEFT JOIN pg_namespace n ON c.relnamespace = n.oid \n" +
                "WHERE c.relkind = 'v'";
        try (HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            //找到所有符合shipList的表和他们的父表/子表
            List<TableInfo> possibleTables = new ArrayList<>();
            Set<String> tables = new HashSet<>();
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(searchTablesSql)) {
                    while (rs.next()) {
                        TableInfo tableInfo = new TableInfo();
                        String schemaName = rs.getString("schemaname");
                        String tableName = rs.getString("tablename");
                        String parentSchema = rs.getString("parentschema");
                        String parentTable = rs.getString("parenttable");
                        tableInfo.schemaName = schemaName;
                        tableInfo.tableName = tableName;
                        if (rs.getString("partrelid") != null) {
                            tableInfo.isPartitioned = true;
                        } else
                            tableInfo.isPartitioned = false;
                        tableInfo.parentSchema = parentSchema;
                        tableInfo.parentTable = parentTable;
                        tableInfo.isForeign = false;
                        tableInfo.isView = false;
                        if (this.filterListContains(shipList, schemaName, tableName) || (parentSchema != null && this.filterListContains(shipList, parentSchema, parentTable))) {
                            if(!tables.contains(schemaName + '.' + tableName)) {
                                possibleTables.add(tableInfo);
                                tables.add(schemaName + '.' + tableName);
                            }
                            if (parentSchema != null && (!tables.contains(parentSchema + '.' + parentTable))) {
                                TableInfo parentInfo = new TableInfo();
                                parentInfo.schemaName = parentSchema;
                                parentInfo.tableName = parentTable;
                                parentInfo.isPartitioned = true;
                                parentInfo.parentSchema = null;
                                parentInfo.parentTable = null;
                                parentInfo.isForeign = false;
                                parentInfo.isView = false;
                                possibleTables.add(parentInfo);
                                tables.add(parentSchema + '.' + parentTable);
                            }
                        }
                    }
                }
                if (restoreForeign) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(searchForeignTablesSql)) {
                        while (rs.next()) {
                            TableInfo tableInfo = new TableInfo();
                            String schemaName = rs.getString("schemaname");
                            String tableName = rs.getString("tablename");
                            tableInfo.schemaName = schemaName;
                            tableInfo.tableName = tableName;
                            tableInfo.isPartitioned = false;
                            tableInfo.parentSchema = null;
                            tableInfo.parentTable = null;
                            tableInfo.isForeign = true;
                            tableInfo.isView = false;
                            if (this.filterListContains(shipList, schemaName, tableName)) {
                                if (!tables.contains(schemaName + '.' + tableName)) {
                                    possibleTables.add(tableInfo);
                                    tables.add(schemaName + '.' + tableName);
                                }
                            }
                        }
                    }
                }
                if (restoreView) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(searchViewsSql)) {
                        while (rs.next()) {
                            TableInfo tableInfo = new TableInfo();
                            String schemaName = rs.getString("schemaname");
                            String tableName = rs.getString("tablename");
                            tableInfo.schemaName = schemaName;
                            tableInfo.tableName = tableName;
                            tableInfo.isPartitioned = false;
                            tableInfo.parentSchema = null;
                            tableInfo.parentTable = null;
                            tableInfo.isForeign = false;
                            tableInfo.isView = true;
                            if (this.filterListContains(shipList, schemaName, tableName)) {
                                if (!tables.contains(schemaName + '.' + tableName)) {
                                    possibleTables.add(tableInfo);
                                    tables.add(schemaName + '.' + tableName);
                                }
                            }
                        }
                    }
                }
                return null;
            }).get();
            //根据blackList filter
            if(blackList != null) {
                for(TableInfo table: possibleTables) {
                    String schemaName = table.schemaName;
                    String tableName = table.tableName;
                    String parentSchema = table.parentSchema;
                    String parentTable = table.parentTable;
                    if (this.filterListContains(blackList, schemaName, tableName) || (parentSchema != null && this.filterListContains(blackList, parentSchema, parentTable)))
                        tables.remove(schemaName + '.' + tableName);
                    else
                        tableInfoList.add(table);
                }
            }else
                tableInfoList = possibleTables;

            tablesMeta.tableInfoList = tableInfoList;
            //get spm and slpm info
            boolean spm = (boolean) client.sql(conn -> {
                String ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("show hg_experimental_enable_spm")) {
                        rs.next();
                        ret = rs.getString("hg_experimental_enable_spm");
                    }
                } catch (Exception e) {
                    LOGGER.warn("No spm info found");
                }
                if(ret != null && ret.equals("on"))
                    return true;
                return false;
            }).get();
            boolean slpm = (boolean) client.sql(conn -> {
                String ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("show hg_enable_slpm")) {
                        rs.next();
                        ret = rs.getString("hg_enable_slpm");
                    }
                } catch (Exception e) {
                    LOGGER.warn("No slpm info found");
                }
                if(ret != null && ret.equals("on"))
                    return true;
                return false;
            }).get();
            if(spm) {
                restoreOwner = false;
                restorePriv = false;
                Map<String, List<String>> spmInfo = new HashMap<>();
                String[] roleGroups = {String.format("%s_admin", dbName), String.format("%s_developer", dbName), String.format("%s_writer", dbName), String.format("%s_viewer", dbName)};
                for(String groupName: roleGroups) {
                    List<String> members = new ArrayList<>();
                    client.sql(conn -> {
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(String.format("select usename from pg_user, pg_group where pg_user.usesysid = ANY(pg_group.grolist) and pg_group.groname='%s'", groupName))) {
                            while(rs.next()) {
                                members.add(rs.getString("usename"));
                            }
                        }
                        return null;
                    }).get();
                    spmInfo.put(groupName, members);
                }
                tablesMeta.spmInfo = spmInfo;
            } else
                tablesMeta.spmInfo = null;
            if(slpm) {
                restoreOwner = false;
                restorePriv = false;
                Map<String, List<String>> slpmInfo = new HashMap<>();
                List<String> roleGroups = new ArrayList<>();
                roleGroups.add(String.format("%s.admin", dbName));
                for(String schemaName: shipList.keySet()) {
                    roleGroups.add(String.format("%s.%s.developer", dbName, schemaName));
                    roleGroups.add(String.format("%s.%s.writer", dbName, schemaName));
                    roleGroups.add(String.format("%s.%s.viewer", dbName, schemaName));
                }
                for(String groupName: roleGroups) {
                    List<String> members = new ArrayList<>();
                    client.sql(conn -> {
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(String.format("select usename from pg_user, pg_group where pg_user.usesysid = ANY(pg_group.grolist) and pg_group.groname='%s'", groupName))) {
                            while(rs.next()) {
                                members.add(rs.getString("usename"));
                            }
                        }
                        return null;
                    }).get();
                    slpmInfo.put(groupName, members);
                }
                tablesMeta.slpmInfo = slpmInfo;
            } else
                tablesMeta.slpmInfo = null;
            //get owner info
            if(restoreOwner) {
                Map<String, String> ownerInfo = new HashMap<>();
                client.sql(conn -> {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT * FROM pg_catalog.pg_tables")) {
                        while(rs.next()) {
                            String schemaName = rs.getString("schemaname");
                            String tableName = rs.getString("tablename");
                            String owner = rs.getString("tableowner");
                            if(tables.contains(schemaName + '.' + tableName)) {
                                ownerInfo.put(schemaName + '.' + tableName, owner);
                            }
                        }
                    }
                    return null;
                }).get();
                tablesMeta.ownerInfo = ownerInfo;
            }
            else
                tablesMeta.ownerInfo = null;
            //get grantees info
            if(restorePriv) {
                String findGranteesSql = "SELECT table_schema, table_name, ARRAY_AGG(DISTINCT grantee) AS grantees\n" +
                                         "FROM information_schema.table_privileges\n" +
                                         "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'hologres') AND grantee != 'PUBLIC'\n" +
                                         "GROUP BY table_schema, table_name";
                Map<String, List<String>> granteesInfo = new HashMap<>();
                client.sql(conn -> {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(findGranteesSql)) {
                        while(rs.next()) {
                            String schemaName = rs.getString("table_schema");
                            String tableName = rs.getString("table_name");
                            Object[] granteesArray = (Object[]) rs.getArray("grantees").getArray();
                            if(tables.contains(schemaName + '.' + tableName)) {
                                List<String> grantees = new ArrayList<>();
                                for(Object grantee : granteesArray)
                                    grantees.add(grantee.toString());
                                granteesInfo.put(schemaName + '.' + tableName, grantees);
                            }
                        }
                    }
                    return null;
                }).get();
                tablesMeta.granteesInfo = granteesInfo;
            }
            else
                tablesMeta.granteesInfo = null;
            LOGGER.info("Finished reading metadata from "+jdbcUrl);
        }catch (Exception e) {
            LOGGER.error("Failed getting metaData from "+jdbcUrl, e);
        }

        return tablesMeta;
    }

    public void recordMetadata(TablesMeta dbInfo) {}

    public AbstractTable getTable(String tableName) throws Exception{
        AbstractTable table = new HoloTable(tableName, jdbcUrl, user, password, version);
        return table;
    }

    public boolean prepareRead() {
        //create extension hg_toolkit
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            this.version  = (HoloVersion)(client.sql(MetaUtil::getHoloVersion)).get();
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("CREATE EXTENSION IF NOT EXISTS hg_toolkit");
                }
                return null;
            }).get();
            return true;
        } catch (Exception e) {
            LOGGER.warn("Failed creating extension hg_toolkit, will fetch table DDL manually");
            return false;
        }
    }

    public void prepareWrite() {
        if(dbName.equals("postgres")) {
            try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
                client.sql(conn -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate("ALTER DATABASE postgres SET hg_experimental_allow_create_table_under_postgres = 'on'");
                    }
                    return null;
                }).get();
            } catch (Exception e)
            {
                LOGGER.error("Failed setting hg_experimental_allow_create_table_under_postgres", e);
            }
        }
    }

    public boolean checkTableExistence(String tableName) {
        String schemaName = tableName.split("\\.",2)[0];
        String pureTableName = tableName.split("\\.",2)[1];
        Boolean existence = null;
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            existence = (Boolean) client.sql(conn -> {
                Boolean ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(String.format("SELECT EXISTS (SELECT FROM pg_catalog.pg_tables WHERE schemaname = '%s' AND tablename = '%s')",schemaName,pureTableName))) {
                        rs.next();
                        ret = rs.getBoolean("exists");
                    }
                }
                return ret;
            }).get();
        } catch (Exception e) {
            LOGGER.error("Failed checking existence for table " + tableName, e);
        }
        return existence;
    }

    public String getGUC() {
        LOGGER.info("Starting fetching GUC from "+jdbcUrl);
        String GUCInfo = null;
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            GUCInfo = (String) client.sql(conn -> {
                String GUC = "BEGIN;\n";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(String.format("SELECT setconfig FROM pg_db_role_setting s LEFT JOIN pg_database d on s.setdatabase = d.oid WHERE setrole = 0 and datname = '%s'",dbName))) {
                    while(rs.next()) {
                        Array configArray = rs.getArray("setconfig");
                        String[] configInfo = (String[])configArray.getArray();
                        for(String config:configInfo) {
                            String name = config.split("=")[0];
                            String value = config.split("=")[1];
                            if(!(name.equals("hg_experimental_enable_spm")||name.equals("hg_enable_slpm"))) //spm和slpm的部分在恢复权限时恢复
                                GUC = GUC + String.format("ALTER DATABASE %s SET %s = '%s';\n",dbName,name,value);
                        }
                    }
                }
                GUC = GUC + "END;\n";
                return GUC;
            }).get();
            LOGGER.info("Finished fetching GUC from "+jdbcUrl);
        } catch (Exception e)
        {
            LOGGER.error("Failed fetching GUC for database "+dbName, e);
        }

        return GUCInfo;
    }

    public void setGUC(String GUCInfo) {
        if(GUCInfo == null) {
            LOGGER.info("No GUC info");
            return;
        }
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(GUCInfo);
                }
                return null;
            }).get();
        } catch (Exception e) {
            LOGGER.error("Failed setting GUC", e);
        }
    }

    public String getExtension() {
        LOGGER.info("Starting fetching extension info from "+jdbcUrl);
        String extInfo = null;
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            extInfo = (String) client.sql(conn -> {
                String ext = "BEGIN;\n";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT extname FROM pg_extension")) {
                    while(rs.next()) {
                        String extName = rs.getString("extname");
                        ext = ext + String.format("CREATE EXTENSION IF NOT EXISTS %s;\n",extName);
                    }
                }
                ext = ext + "END;\n";
                return ext;
            }).get();
            LOGGER.info("Finished fetching extension info from "+jdbcUrl);
        } catch (Exception e)
        {
            LOGGER.error("Failed fetching extension info for database "+dbName, e);
        }

        return extInfo;
    }
    public void setExtension(String extInfo) {
        if(extInfo == null) {
            LOGGER.info("No extension info");
            return;
        }
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(extInfo);
                }
                return null;
            }).get();
        } catch (Exception e)
        {
            LOGGER.error("Failed setting extensions", e);
        }
    }
    public void createSchemas(List<String> schemaList) {
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    for(String schema:schemaList) {
                        stmt.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
                    }
                }
                return null;
            }).get();
        } catch (Exception e)
        {
            LOGGER.error("Failed creating schemas", e);
        }
    }

    public void restoreSPM(Map<String, List<String>> spmInfo) {
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            //开启spm
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    String spm = null;
                    try (ResultSet rs = stmt.executeQuery("show hg_experimental_enable_spm")) {
                        rs.next();
                        spm = rs.getString("hg_experimental_enable_spm");
                    }
                    if(!spm.equals("on"))
                        stmt.executeUpdate("call spm_enable ()");
                    for(String groupName : spmInfo.keySet()) {
                        for(String username : spmInfo.get(groupName))
                            stmt.executeUpdate(String.format("call spm_grant ('%s', '%s')", groupName, username));
                    }
                }
                return null;
            }).get();
        } catch (Exception e)
        {
            LOGGER.error("Failed restoring SPM", e);
        }
    }
    public void restoreSLPM(Map<String, List<String>> slpmInfo) {
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            //开启slpm
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    String slpm = null;
                    try (ResultSet rs = stmt.executeQuery("show hg_enable_slpm")) {
                        rs.next();
                        slpm = rs.getString("hg_enable_slpm");
                    }
                    if(!slpm.equals("on"))
                        stmt.executeUpdate("call slpm_enable ()");
                    for(String groupName : slpmInfo.keySet()) {
                        for(String username : slpmInfo.get(groupName))
                            stmt.executeUpdate(String.format("call slpm_grant ('%s', '%s')", groupName, username));
                    }
                }
                return null;
            }).get();
        } catch (Exception e)
        {
            LOGGER.error("Failed restoring SLPM", e);
        }
    }

}
