/*
 * Copyright (c) 2025. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.auth;

import org.postgresql.PGProperty;
import org.postgresql.plugin.AuthenticationPlugin;
import org.postgresql.plugin.AuthenticationRequestType;
import org.postgresql.util.PSQLException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AKv4AuthenticationPlugin implements AuthenticationPlugin {
    public static final String REGION = "HGREGION";
    public static final String AKV4_PREFIX = "akv4$";

    private String user;
    private String password;
    private String host;
    private String region;
    private String product;

    public AKv4AuthenticationPlugin(Properties props) {
        user = PGProperty.USER.get(props);
        if (user == null) {
            throw new IllegalArgumentException(
                    "Unable to extract user, you may specify PGProperty.USER in connection properties");
        }

        password = PGProperty.PASSWORD.get(props);
        if (password == null) {
            throw new IllegalArgumentException(
                    "Unable to extract password, you may specify PGProperty.PASSWORD in connection properties");
        }

        host = PGProperty.PG_HOST.get(props);
        region = props.getProperty(REGION);
        if (region == null) {
            region = extractRegionFromHost(host);
            if (region == null) {
                throw new IllegalArgumentException(
                        "Unable to extract region from host, you may specify AKv4AuthenticationPlugin.REGION in connection properties");
            }
        }

        product = "hologres";
    }

    @Override
    public char[] getPassword(AuthenticationRequestType type) throws PSQLException {
        if (user.startsWith(AKV4_PREFIX)) {
            return encodeAKv4Password(password, region, product).toCharArray();
        } else {
            return password.toCharArray();
        }
    }

    private static final Pattern regionPattern =
            Pattern.compile(
                    "^[^-]+-[^-]+-[^-]+-(.+?)(?:-internal|-vpc|-vpc-st)?\\.hologres\\.aliyuncs\\.com$");

    public static String extractRegionFromHost(String host) {
        if (host == null) {
            return null;
        }
        Matcher matcher = regionPattern.matcher(host);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }

    public static String encodeAKv4Password(String secret, String region, String product) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String date = sdf.format(new Date());
            String str = "aliyun_v4" + secret;
            Mac h = Mac.getInstance("HmacSHA256");
            SecretKeySpec k1 =
                    new SecretKeySpec(str.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            h.init(k1);
            byte[] bytes1 = h.doFinal(date.getBytes(StandardCharsets.UTF_8));
            h.reset();
            SecretKeySpec k2 = new SecretKeySpec(bytes1, "HmacSHA256");
            h.init(k2);
            byte[] bytes2 = h.doFinal(region.getBytes(StandardCharsets.UTF_8));
            h.reset();
            SecretKeySpec k3 = new SecretKeySpec(bytes2, "HmacSHA256");
            h.init(k3);
            byte[] bytes3 = h.doFinal(product.getBytes(StandardCharsets.UTF_8));
            h.reset();
            SecretKeySpec k4 = new SecretKeySpec(bytes3, "HmacSHA256");
            h.init(k4);
            byte[] bytes4 = h.doFinal("aliyun_v4_request".getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(bytes4);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to encode AKv4 password", e);
        }
    }
}
