package com.alibaba.hologres.shipper.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SqlUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqlUtil.class);

    public static Map<String, String> getGUC(Connection conn, String dbName) throws Exception{
        String sql = "SELECT setconfig FROM pg_db_role_setting s LEFT JOIN pg_database d on s.setdatabase = d.oid WHERE setrole = 0 and datname = ?";
        Map<String, String> gucMapping = new HashMap<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, dbName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Array configArray = rs.getArray("setconfig");
                    String[] configInfo = (String[]) configArray.getArray();
                    for (String config : configInfo) {
                        String[] guc_kv = config.split("=");
                        if (guc_kv.length != 2) {
                            LOGGER.error("find guc not parse, key {}", config);
                            throw new Exception("find guc not parse");
                        }
                        String name = guc_kv[0];
                        String value = guc_kv[1];
                        //spm和slpm的部分在恢复权限时恢复
                        if (!(name.equals("hg_experimental_enable_spm") || name.equals("hg_enable_slpm"))) {
                            gucMapping.put(name, value);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("failed to get guc database:{}", dbName, e);
        }
        return gucMapping;
    }

    public static void setGUC(Connection conn, String dbName, Map<String, String> gucMapping) {
        try (Statement stmt = conn.createStatement()) {
            for(Map.Entry<String, String> entry : gucMapping.entrySet()) {
                String sql = "ALTER DATABASE \"" + dbName + "\" SET " + entry.getKey() + " = " + entry.getValue();
                LOGGER.info("set guc {}", sql);
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            LOGGER.error("failed to set guc database:{}", dbName, e);
        }
        return;
    }
}
