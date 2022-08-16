package com.alibaba.hologres.shipper.holo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.shipper.HoloDBShipper;

public class HoloUtils {

    public static HoloClient getHoloClient(String jdbcUrl, String user, String password) throws HoloClientException{
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        HoloClient client = new HoloClient(config);
        return client;
    }

    public static HoloClient getHoloClient(String jdbcUrl, String user, String password, int writeThreadSize) throws HoloClientException{
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setWriteThreadSize(writeThreadSize);
        HoloClient client = new HoloClient(config);
        return client;
    }

    public static HoloClient getSimpleClient(String jdbcUrl, String user, String password) throws HoloClientException{
        jdbcUrl += "?preferQueryMode=simple";
        HoloConfig config = new HoloConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        HoloClient client = new HoloClient(config);
        return client;
    }

    public static String getJdbcUrl(String ip, String port, String dbName)
    {
        return String.format("jdbc:postgresql://%s:%s/%s", ip, port, dbName);
    }

    public static boolean needsToShip(String schemaName) {
        List<String> schemasToExclude = Arrays.asList("hologres", "information_schema", "hologres_sample", "hologres_statistic");
        if(schemaName.startsWith("pg_")||schemasToExclude.contains(schemaName))
            return false;
        return true;
    }

    public static String getTableNameWithQuotes(String tableName) {
        String schemaName = tableName.split("\\.",2)[0];
        String pureTableName = tableName.split("\\.",2)[1];
        return String.format("\"%s\".\"%s\"",schemaName,pureTableName);
    }


}
