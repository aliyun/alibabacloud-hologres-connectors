package com.alibaba.hologres.kafka.utils;

import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.kafka.conf.HoloSinkConfigManager;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

/** JDBCUtils. */
public class JDBCUtils {
    private static final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);

    public static boolean couldDirectConnect(HoloSinkConfigManager configManager) {
        Properties info = new Properties();
        PGProperty.USER.set(info, configManager.getHoloConfig().getUsername());
        PGProperty.PASSWORD.set(info, configManager.getHoloConfig().getPassword());
        PGProperty.SOCKET_TIMEOUT.set(info, 5);
        PGProperty.APPLICATION_NAME.set(info, "hologres-connector-kafka_copy");
        if (configManager.getHoloConfig().isUseAKv4()) {
            setAKv4Region(info, configManager.getHoloConfig().getRegion());
        }
        boolean couldDirectConnect = false;
        try {
            String directUrl =
                    ConnectionUtil.getDirectConnectionUrl(
                            configManager.getHoloConfig().getJdbcUrl(), info, false);
            couldDirectConnect = !directUrl.equals(configManager.getHoloConfig().getJdbcUrl());
        } catch (SQLException ignored) {
        }
        return couldDirectConnect;
    }

    public static String formatUrlWithHologres(String oldUrl) {
        String url = oldUrl;
        // the copyWriter just supports jdbc:hologres
        if (oldUrl != null && oldUrl.startsWith("jdbc:postgresql:")) {
            url = "jdbc:hologres:" + oldUrl.substring("jdbc:postgresql:".length());
        }
        return url;
    }

    public static void setAKv4Region(Properties info, String akv4Region) {
        PGProperty.USER.set(info, AKv4AuthenticationPlugin.AKV4_PREFIX + PGProperty.USER.get(info));
        PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                info, AKv4AuthenticationPlugin.class.getName());
        if (akv4Region != null && !akv4Region.isEmpty()) {
            info.setProperty(AKv4AuthenticationPlugin.REGION, akv4Region);
        }
    }
}
