package com.alibaba.hologres.connector.flink.example;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SettingHelper {
    private static final transient Logger LOG = LoggerFactory.getLogger(SettingHelper.class);

    String endpoint;
    String username;
    String password;
    String database;
    String writeMode;

    SettingHelper(String[] args, String propsPath) {
        try {
            CommandLineParser parser = new DefaultParser();
            Options options = new Options();
            CommandLine commandLine = parser.parse(options, args);
            endpoint = commandLine.getOptionValue("endpoint");
            username = commandLine.getOptionValue("username");
            password = commandLine.getOptionValue("password");
            database = commandLine.getOptionValue("database");
        } catch (ParseException e) {
            LOG.warn("couldn't parse job setting from args, try read properties file", e);
        }
        if (IsValid()) {
            return;
        }
        try {
            LOG.info("parse job setting from project local properties file {}", propsPath);
            // 从配置文件中读取参数, 项目路径
            Properties props = new Properties();
            InputStream inputStream = SettingHelper.class.getClassLoader().getResourceAsStream(propsPath);
            props.load(inputStream);

            // 参数，包括维表dim与结果表dws的名称
            endpoint = props.getProperty("endpoint");
            username = props.getProperty("username");
            password = props.getProperty("password");
            database = props.getProperty("database");
            writeMode = props.getProperty("writemode");
        } catch (IOException ex) {
            LOG.warn("couldn't parse job setting from properties file", ex);
        }
        if (IsValid()) {
            return;
        }
        try {
            LOG.info("parse job setting from absolute path properties file {}", propsPath);
            // 从配置文件中读取参数, 绝对路径
            Properties props = new Properties();
            InputStream inputStream = new FileInputStream(propsPath);
            props.load(inputStream);

            // 参数，包括维表dim与结果表dws的名称
            endpoint = props.getProperty("endpoint");
            username = props.getProperty("username");
            password = props.getProperty("password");
            database = props.getProperty("database");
            writeMode = props.getProperty("writemode");
        } catch (IOException ex) {
            LOG.warn("couldn't parse job setting from properties file", ex);
        }
        if (!IsValid()) {
            throw new RuntimeException("count parse job setting from args or properties file");
        }
    }

    public String getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getWriteMode() {
        return writeMode;
    }

    private boolean IsValid() {
        return database != null &&
                password != null &&
                username != null &&
                endpoint != null;
    }


}
