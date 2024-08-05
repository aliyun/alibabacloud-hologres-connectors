package com.alibaba.hologres.hive.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/** Main configuration handler class. */
public class HoloStorageConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloStorageConfigManager.class);

    public static void copyConfigurationToJob(Properties props, Map<String, String> jobProps) {
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            LOGGER.info("copy conf to job:{}={}", key, value);
            if (key.startsWith("hive.sql.")) {
                jobProps.put(key, value);
            }
        }
    }
}
