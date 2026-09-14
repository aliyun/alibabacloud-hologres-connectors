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

    private static final String HOLO_SUFFIX = ".hologres.aliyuncs.com";

    /** 需要在匹配 region 之前剥掉的网络类型后缀, 长的放前面, 避免 -vpc 抢先匹配 -vpc-st. */
    private static final String[] NETWORK_SUFFIXES = {"-internal", "-vpc-st", "-vpc"};

    /** cn-beijing / cn-hongkong / ap-southeast-1 / cn-shanghai-finance-1 / eu-central-1 ... */
    private static final Pattern regionPattern =
            Pattern.compile("(?:cn|ap|us|eu|me|na)-[a-z]+(?:-[a-z]+)*(?:-\\d+)?");

    /**
     * 从 host 中抽取 region.
     *
     * <p>做法是从右往左取后缀, 返回最短的合法 region. 早先的实现用 {@code ^[^-]+-[^-]+-[^-]+-(.+?)...} 跳过固定的 3 段前缀,
     * 对前缀段数不等于 3 的 endpoint 会抽错: holohub-catalog-cn-beijing.hologres.aliyuncs.com 前缀只有 2 段, "cn"
     * 被当成第 3 段前缀吃掉, 只抽出 "beijing". region 参与 AKv4 签名的密钥派生, 抽错的直接后果是服务端报 password authentication
     * failed, 排查时完全指不到真正原因.
     *
     * <p>也不能直接用贪婪正则在末尾匹配: 对 hgprecn-cn-test-cn-shanghai-finance-1 会一路吞成
     * cn-test-cn-shanghai-finance-1.
     *
     * @return 抽取到的 region; host 为 null、非 Hologres 公网域名或形态不可识别时返回 null
     */
    public static String extractRegionFromHost(String host) {
        if (host == null || !host.endsWith(HOLO_SUFFIX)) {
            return null;
        }
        String prefix = host.substring(0, host.length() - HOLO_SUFFIX.length());
        for (String networkSuffix : NETWORK_SUFFIXES) {
            if (prefix.endsWith(networkSuffix)) {
                prefix = prefix.substring(0, prefix.length() - networkSuffix.length());
                break;
            }
        }
        String[] segments = prefix.split("-");
        for (int n = 2; n <= segments.length; n++) {
            StringBuilder candidate = new StringBuilder(segments[segments.length - n]);
            for (int i = segments.length - n + 1; i < segments.length; i++) {
                candidate.append("-").append(segments[i]);
            }
            Matcher matcher = regionPattern.matcher(candidate);
            if (matcher.matches()) {
                return candidate.toString();
            }
        }
        return null;
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
