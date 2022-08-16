package com.alibaba.hologres.shipper.holo;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.org.postgresql.util.HoloVersion;
import com.alibaba.hologres.org.postgresql.util.MetaUtil;
import com.alibaba.hologres.shipper.HoloShipper;
import com.alibaba.hologres.shipper.generic.AbstractDB;
import com.alibaba.hologres.shipper.generic.AbstractInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class HoloInstance extends AbstractInstance{
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloInstance.class);
    String ip;
    String port;
    String user;
    String password;
    String jdbcUrl;

    public HoloInstance(String ip, String port, String user, String password) {
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
        this.jdbcUrl = HoloUtils.getJdbcUrl(ip, port, "postgres");
    }

    public List<String> getDBList() {
        List<String> dbList = new ArrayList<String>();
        LOGGER.info("Starting getting DBList from "+jdbcUrl);
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT datname FROM pg_database")) {
                    while(rs.next()) {
                        String dbName = rs.getString("datname");
                        if(!(dbName.equals("template0")||dbName.equals("template1")))
                            dbList.add(dbName);
                    }
                }
                return null;
            }).get();
            LOGGER.info("Finished getting DBList");
        } catch (Exception e)
        {
            LOGGER.error("Failed fetching db list from "+jdbcUrl, e);
        }
        return dbList;
    }

    public AbstractDB getDB(String dbName) {
        return new HoloDB(HoloUtils.getJdbcUrl(ip, port, dbName), user, password, dbName);
    }

    public List<String> getAllRoles() {
        List<String> roleList = new ArrayList<String>();
        LOGGER.info("Starting getting roles from "+jdbcUrl);
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT usename FROM pg_user")) {
                    while(rs.next()) {
                        String roleName = rs.getString("usename");
                        if(!roleName.equals("holo_admin"))
                            roleList.add(roleName);
                    }
                }
                return null;
            }).get();
            LOGGER.info("Finished getting roles");
        } catch (Exception e)
        {
            LOGGER.error("Failed fetching roles list from "+jdbcUrl, e);
        }
        return roleList;
    }

    public void createRoles(List<String> roles, Map<String, Boolean> roleInfo) {
        Set<String> existingRoles = new HashSet<>();
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT rolname FROM pg_roles")) {
                    while(rs.next()) {
                        existingRoles.add(rs.getString("rolname"));
                    }
                }
                return null;
            }).get();
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    for(String role:roles) {
                        if(!existingRoles.contains(role)) {
                            String roleType = "ROLE";
                            if(roleInfo.containsKey(role) && roleInfo.get(role))
                                roleType = "USER";
                            stmt.executeUpdate(String.format("CREATE %s \"%s\"", roleType, role));
                        }
                    }
                }
                return null;
            }).get();
        } catch (Exception e)
        {
            LOGGER.error("Failed creating roles", e);
        }
    }

    public void showVersion() {
        java.util.Date date = new Date();
        LOGGER.info("=========holo-shipper version==========\n");
        LOGGER.info("version: " + HoloShipper.VERSION + "\n");
        LOGGER.info("date: " + date.toString() + "\n");
        LOGGER.info("=======================================\n");
    }

    public void createDB(String dbName) {
        try(HoloClient client = HoloUtils.getHoloClient(jdbcUrl, user, password)) {
            Boolean existence = (Boolean) client.sql(conn -> {
                Boolean ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(String.format("SELECT EXISTS (SELECT FROM pg_database WHERE datname = '%s')",dbName))) {
                        rs.next();
                        ret = rs.getBoolean("exists");
                    }
                }
                return ret;
            }).get();
            if(!existence) {
                client.sql(conn -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate(String.format("CREATE DATABASE %s", dbName));
                    }
                    return null;
                }).get();
            }
        } catch (Exception e) {
            LOGGER.error("Failed creating database " + dbName, e);
        }
    }

    public Map<String, Boolean> getRoleInfo(List<String> roleList){
        //true: role can login, false: role cannot login
        Map<String, Boolean> roleInfo = new HashMap<>();
        List<String> userList = getAllRoles();
        for(String role : roleList) {
            roleInfo.put(role, userList.contains(role));
        }
        return roleInfo;
    }

}
