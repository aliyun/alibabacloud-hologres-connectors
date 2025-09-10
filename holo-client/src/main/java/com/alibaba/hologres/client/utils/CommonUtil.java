package com.alibaba.hologres.client.utils;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.min;

/** Created by liangmei.gl. Date: 2020-12-23 11:18 */
public class CommonUtil {

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isEmpty(Object[] cs) {
        return cs == null || cs.length == 0;
    }

    public static String join(Object[] array, String separator) {
        return array == null ? null : join((Object[]) array, separator, 0, array.length);
    }

    public static boolean isNotEmpty(Object[] cs) {
        return !isEmpty(cs);
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    public static String join(Object[] array, String separator, int startIndex, int endIndex) {
        if (array == null) {
            return null;
        } else {
            if (separator == null) {
                separator = "";
            }

            int noOfItems = endIndex - startIndex;
            if (noOfItems <= 0) {
                return "";
            } else {
                StringBuilder buf = newStringBuilder(noOfItems);
                if (array[startIndex] != null) {
                    buf.append(array[startIndex]);
                }

                for (int i = startIndex + 1; i < endIndex; ++i) {
                    buf.append(separator);
                    if (array[i] != null) {
                        buf.append(array[i]);
                    }
                }

                return buf.toString();
            }
        }
    }

    private static StringBuilder newStringBuilder(int noOfItems) {
        return new StringBuilder(noOfItems * 16);
    }

    public static String join(Iterable<?> iterable, String separator) {
        return iterable == null ? null : join(iterable.iterator(), separator);
    }

    public static String join(Iterator<?> iterator, String separator) {
        if (iterator == null) {
            return null;
        } else if (!iterator.hasNext()) {
            return "";
        } else {
            Object first = iterator.next();
            if (!iterator.hasNext()) {
                String result = toString(first);
                return result;
            } else {
                StringBuilder buf = new StringBuilder(256);
                if (first != null) {
                    buf.append(first);
                }

                while (iterator.hasNext()) {
                    if (separator != null) {
                        buf.append(separator);
                    }

                    Object obj = iterator.next();
                    if (obj != null) {
                        buf.append(obj);
                    }
                }

                return buf.toString();
            }
        }
    }

    public static String toString(Object obj) {
        return obj == null ? "" : obj.toString();
    }

    // pg_utf8_verifier
    private static int pgUtf8Verifier(String input, int startIndex, int len) {
        int utfLen = pgUtfMblen(input.charAt(startIndex));
        if (utfLen > len) {
            return -1;
        }

        if (!pgUtf8IsLegal(input, startIndex, utfLen)) {
            return -1;
        }
        return utfLen;
    }

    // pg_utf_mblen
    private static int pgUtfMblen(char ch) {
        int len;

        if ((ch & 0x80) == 0) len = 1;
        else if ((ch & 0xe0) == 0xc0) len = 2;
        else if ((ch & 0xf0) == 0xe0) len = 3;
        else if ((ch & 0xf8) == 0xf0) len = 4;
        else len = 1;
        return len;
    }
    // pg_utf8_islegal
    private static boolean pgUtf8IsLegal(String input, int startIndex, int utfLen) {
        char a;

        switch (utfLen) {
            default:
                /* reject lengths 5 and 6 for now */
                return false;
            case 4:
                a = input.charAt(startIndex + 3);
                if (a < 0x80 || a > 0xBF) return false;
                /* FALL THRU */
            case 3:
                a = input.charAt(startIndex + 2);
                if (a < 0x80 || a > 0xBF) return false;
                /* FALL THRU */
            case 2:
                a = input.charAt(startIndex + 1);
                switch (input.charAt(startIndex)) {
                    case 0xE0:
                        if (a < 0xA0 || a > 0xBF) return false;
                        break;
                    case 0xED:
                        if (a < 0x80 || a > 0x9F) return false;
                        break;
                    case 0xF0:
                        if (a < 0x90 || a > 0xBF) return false;
                        break;
                    case 0xF4:
                        if (a < 0x80 || a > 0x8F) return false;
                        break;
                    default:
                        if (a < 0x80 || a > 0xBF) return false;
                        break;
                }
                /* FALL THRU */
            case 1:
                a = input.charAt(startIndex);
                if (a >= 0x80 && a < 0xC2) return false;
                if (a > 0xF4) return false;
                break;
        }
        return true;
    }

    // pg_verify_mbstr_len
    public static int pgVerifyMbstrLen(String input) throws SQLException {
        if (input == null || input.isEmpty()) {
            return 0;
        }
        int len = input.length();
        int mbLen = 0;
        int startIndex = 0;
        while (len > 0) {
            if ((input.charAt(startIndex) & 0x80) == 0) {
                if (input.charAt(startIndex) != '\0') {
                    startIndex++;
                    mbLen++;
                    len--;
                    continue;
                }
                ReportInvalidEncoding(input, startIndex, len);
            }
            int l;
            l = pgUtf8Verifier(input, startIndex, len);
            if (l < 0) {
                ReportInvalidEncoding(input, startIndex, len);
            }
            startIndex += l;
            len -= l;
            mbLen++;
        }
        return mbLen;
    }

    // report_invalid_encoding
    private static void ReportInvalidEncoding(String input, int startIndex, int len)
            throws SQLException {
        int utfLen = pgUtfMblen(input.charAt(startIndex));
        int limit = min(len, utfLen);
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < limit; j++) {
            sb.append(String.format("0x%02x", (int) input.charAt(startIndex + j)));
            if (j < limit - 1) {
                sb.append(" ");
            }
        }
        throw new SQLException(
                String.format("invalid byte sequence for encoding \"UTF8\": %s", sb.toString()),
                "22021"); // ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
    }

    // pg_mbstrlen_with_len
    private static int pgMbstrlenWithLen(String input, int startIndex, int limit) {
        int len = 0;

        while (limit > 0 && input.charAt(startIndex) != 0) {
            int l = pgUtfMblen(input.charAt(startIndex));

            limit -= l;
            startIndex += l;
            len++;
        }
        return len;
    }

    // pg_mbcharcliplen
    private static int pgMbcharcliplen(String input, int startIndex, int len, int limit) {
        int clen = 0;
        int nch = 0;
        int l;

        while (len > 0 && input.charAt(startIndex) != 0) {
            l = pgUtfMblen(input.charAt(startIndex));
            nch++;
            if (nch > limit) break;
            clen += l;
            len -= l;
            startIndex += l;
        }
        return clen;
    }

    // bpchar_input
    public static String bpcharInput(String input, int maxLen) throws SQLException {
        int len = input.length();

        if (maxLen == len) {
            return input;
        }

        int charLen = pgMbstrlenWithLen(input, 0, len);
        if (charLen > maxLen) {
            int mbMaxLen = pgMbcharcliplen(input, 0, len, maxLen);
            for (int j = mbMaxLen; j < len; j++) {
                if (input.charAt(j) != ' ') {
                    throw new SQLException(
                            String.format("value too long for type character(%d)", maxLen),
                            "22001"); // ERRCODE_STRING_DATA_RIGHT_TRUNCATION
                }
                maxLen = len = mbMaxLen;
            }
        } else {
            maxLen = len + (maxLen - charLen);
        }

        char[] buffer = new char[maxLen];
        for (int i = 0; i < len; i++) {
            buffer[i] = input.charAt(i);
        }
        for (int i = len; i < maxLen; i++) {
            buffer[i] = ' ';
        }
        return new String(buffer);
    }

    // varchar_input
    public static String varcharInput(String input, int maxLen) throws SQLException {
        int len = input.length();

        if (len > maxLen) {
            int mbMaxLen = pgMbcharcliplen(input, 0, len, maxLen);
            for (int j = mbMaxLen; j < len; j++) {
                if (input.charAt(j) != ' ') {
                    throw new SQLException(
                            String.format("value too long for type character varying(%d)", maxLen),
                            "22001"); // ERRCODE_STRING_DATA_RIGHT_TRUNCATION
                }
            }
            len = mbMaxLen;
            return input.substring(0, len);
        }
        return input;
    }

    public static long randomConnectionMaxAliveMs(long maxAliveMs) {
        // 连接至少存活5分钟
        long connectionMaxAliveMs = Math.max(maxAliveMs, 5 * 60 * 1000L);
        // 防止多个连接一起关闭, 随机减少5%以内的时间
        return connectionMaxAliveMs
                - ThreadLocalRandom.current().nextLong(connectionMaxAliveMs / 20);
    }

    public static String encodeColumnNamesToString(String[] columnNames) {
        StringBuilder builder = new StringBuilder();
        // Iterating through the array and building the quoted column names
        for (int i = 0; i < columnNames.length; i++) {
            if (i > 0) {
                builder.append(",");
            }
            // Multi columns names are encoded as a string, it will be executed in the START LOGICAL
            // REPLICATION SQL as a slot option.
            // Like (START_REPLICATION SLOT null LOGICAL 0/0 ("parallel_index" '2', ...
            // "columns_names" '"a","b"'))).
            // As above, it will be passed in through '', so ' also needs to be escaped too.
            if (columnNames[i].contains("'")) {
                columnNames[i] = columnNames[i].replaceAll("'", "''");
            }
            builder.append(
                    IdentifierUtil.quoteIdentifier(
                            columnNames[i],
                            true,
                            true)); // Assuming quoteIdentifier is a method in the same class
        }
        return builder.toString();
    }
}
