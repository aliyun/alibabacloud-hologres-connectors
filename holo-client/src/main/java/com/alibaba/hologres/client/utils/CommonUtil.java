package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    /**
     * Parse the comma-separated quoted logical partition column names string into an array.
     *
     * <p>Each column name must be wrapped in double quotes, and any embedded double quote inside a
     * name must be escaped by doubling it. Whitespace outside double quotes is skipped.
     *
     * <p>Used by both binlog subscription (logical_partition_column_names slot option) and stage
     * INSERT into a logical partition table (target partition column names).
     *
     * <p>Examples: {@code "ds"} -> {@code ["ds"]}; {@code "ds", "kind"} -> {@code ["ds", "kind"]};
     * {@code "co,lu", "mn""x"} -> {@code ["co,lu", "mn\"x"]}.
     */
    public static String[] parseLogicalPartitionColumnNames(String columnNameStr) {
        List<String> columnNames = new ArrayList<>();
        StringBuilder currentIdentifier = new StringBuilder();
        boolean insideQuotes = false;

        for (int i = 0; i < columnNameStr.length(); ++i) {
            char ch = columnNameStr.charAt(i);

            if (insideQuotes) {
                if (ch == '"') {
                    // Check if next character is also a quote
                    if (i + 1 < columnNameStr.length() && columnNameStr.charAt(i + 1) == '"') {
                        currentIdentifier.append('"'); // Append a single quote
                        i++; // Skip the next quote
                    } else {
                        // End of quoted identifier
                        insideQuotes = false;
                    }
                } else {
                    currentIdentifier.append(ch);
                }
            } else {
                if (ch == '"') {
                    // Start of quoted identifier
                    insideQuotes = true;
                } else if (ch == ',') {
                    // Add completed identifier to the list
                    columnNames.add(currentIdentifier.toString());
                    currentIdentifier.setLength(0);
                } else if (Character.isWhitespace(ch)) {
                    // Skip whitespace outside quotes
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid partition column names input: %s, do you wrap each column name with double quotes?",
                                    columnNameStr));
                }
            }
        }

        // Add the last identifier if there was one
        if (!insideQuotes && currentIdentifier.length() > 0) {
            columnNames.add(currentIdentifier.toString());
        }

        if (insideQuotes) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid partition column names input: %s, unterminated quoted identifier",
                            columnNameStr));
        }

        if (!columnNameStr.isEmpty() && columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition column names input: %s", columnNameStr));
        }

        return columnNames.toArray(new String[0]);
    }

    /**
     * Parse the quoted logical partition values string into a 2D array. Columns within a partition
     * are separated by commas; multiple partitions are separated by semicolons. Each value must be
     * wrapped in double quotes and embedded double quotes are escaped by doubling. Whitespace
     * outside double quotes is skipped.
     *
     * <p>Used by both binlog subscription (logical_partition_column_values slot option) and stage
     * INSERT into a logical partition table (target partition values).
     *
     * <p>Example: {@code "2025-01-11", "100"; "2025-01-12", "200"} -> {@code [["2025-01-11",
     * "100"], ["2025-01-12", "200"]]}.
     */
    public static String[][] parseLogicalPartitionColumnValues(String columnValuesStr) {
        List<List<String>> columnValuesForMultiRows = new ArrayList<>();
        List<String> columnValues = new ArrayList<>();
        StringBuilder currentIdentifier = new StringBuilder();
        boolean insideQuotes = false;

        for (int i = 0; i < columnValuesStr.length(); ++i) {
            char ch = columnValuesStr.charAt(i);
            if (insideQuotes) {
                if (ch == '"') {
                    // Check if the next character is also a quote
                    if (i + 1 < columnValuesStr.length() && columnValuesStr.charAt(i + 1) == '"') {
                        currentIdentifier.append('"'); // Append a single quote
                        i++; // Skip the next quote
                    } else {
                        // End of quoted identifier
                        insideQuotes = false;
                    }
                } else {
                    currentIdentifier.append(ch);
                }
            } else {
                if (ch == '"') {
                    // Start of quoted identifier
                    insideQuotes = true;
                } else if (ch == ',') {
                    // Add completed identifier to the list
                    columnValues.add(currentIdentifier.toString());
                    currentIdentifier.setLength(0);
                } else if (ch == ';') {
                    columnValues.add(currentIdentifier.toString());
                    columnValuesForMultiRows.add(new ArrayList<>(columnValues));
                    columnValues.clear();
                    currentIdentifier.setLength(0);
                } else if (Character.isWhitespace(ch)) {
                    // Skip whitespace outside quotes
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid partition values input: %s, do you wrap each value with double quotes?",
                                    columnValuesStr));
                }
            }
        }

        // Add the last identifier if there was one
        if (!insideQuotes && currentIdentifier.length() > 0) {
            columnValues.add(currentIdentifier.toString());
            columnValuesForMultiRows.add(columnValues);
        }

        if (insideQuotes) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid partition values input: %s, unterminated quoted value",
                            columnValuesStr));
        }

        if (!columnValuesStr.isEmpty() && columnValuesForMultiRows.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition values input: %s", columnValuesStr));
        }

        String[][] columnValuesArray = new String[columnValuesForMultiRows.size()][];
        for (int i = 0; i < columnValuesForMultiRows.size(); i++) {
            columnValuesArray[i] = columnValuesForMultiRows.get(i).toArray(new String[0]);
        }
        return columnValuesArray;
    }

    /**
     * 检测是否为Shading环境.
     *
     * @return 是否为Shading环境
     * @throws HoloClientException 如果检测失败
     */
    public static boolean detectShadingEnvironment() throws HoloClientException {
        try {
            DriverManager.getDrivers();
            Class.forName("com.alibaba.hologres.org.postgresql.Driver");
            return true;
        } catch (Exception e) {
            try {
                DriverManager.getDrivers();
                Class.forName("org.postgresql.Driver");
                return false;
            } catch (Exception e2) {
                throw new HoloClientException(ExceptionCode.INTERNAL_ERROR, "load driver fail", e);
            }
        }
    }
}
