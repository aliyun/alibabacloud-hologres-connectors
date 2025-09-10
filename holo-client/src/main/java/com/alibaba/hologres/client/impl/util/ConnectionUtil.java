/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.util;

import com.alibaba.hologres.client.auth.AKv4AuthenticationPlugin;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.Partition;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import com.alibaba.hologres.client.utils.Tuple;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** connection工具类. */
public class ConnectionUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtil.class);
    static Pattern holoVersionPattern = Pattern.compile("release-([^ )]*)");
    static Pattern hgVersionPattern = Pattern.compile("Hologres ([^ -]*)");
    private static String optionProperty = "options=";
    private static String fixedOption = "type=fixed%20";

    public static void refreshMeta(Connection conn, int timeout) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            stat.execute("select hologres.hg_internal_refresh_meta(" + timeout + ")");
        }
    }

    /** CheckMeta的结果. */
    public static class CheckMetaResult {
        boolean updated; // 是否最新
        String msg; // 不是最新的话，原因是什么

        public CheckMetaResult(boolean updated, String msg) {
            this.updated = updated;
            this.msg = msg;
        }

        public boolean isUpdated() {
            return updated;
        }

        public String getMsg() {
            return msg;
        }
    }

    // 目前还有问题，哪个版本修复不确定，拍个2.0.0先，后面版本确定了再改
    private static final HoloVersion CHECK_TABLE_META_SUPPORTED_MIN_VERSION =
            new HoloVersion(1, 1, 50);

    public static CheckMetaResult checkMeta(
            Connection conn, HoloVersion version, String fullName, int timeout)
            throws SQLException {
        if (version.compareTo(CHECK_TABLE_META_SUPPORTED_MIN_VERSION) >= 0) {

            try (Statement stat = conn.createStatement()) {
                stat.setQueryTimeout(timeout);
                try (ResultSet rs =
                        stat.executeQuery(
                                "select hologres.hg_internal_check_table_meta('"
                                        + fullName
                                        + "')")) {
                    if (rs.next()) {
                        String msg = rs.getString(1);
                        boolean ok = "Check meta succeeded".equals(msg);
                        if (ok) {
                            return new CheckMetaResult(ok, msg);
                        } else {
                            refreshMeta(conn, timeout);
                            return new CheckMetaResult(true, null);
                        }
                    } else {
                        return new CheckMetaResult(
                                false, "hologres.hg_internal_check_table_meta return 0 rows");
                    }
                } catch (SQLException e) {
                    if (PSQLState.UNDEFINED_TABLE.getState().equals(e.getSQLState())) {
                        refreshMeta(conn, timeout);
                        return new CheckMetaResult(true, null);
                    } else {
                        throw e;
                    }
                }
            } catch (SQLException e) {
                if (PSQLState.QUERY_CANCELED.getState().equals(e.getSQLState())) {
                    return new CheckMetaResult(
                            false,
                            "table is lock by other query which request a AccessExclusiveLock");
                } else if (e.getMessage().contains("Failed to get table")
                        && e.getMessage().contains("from StoreMaster")) {
                    return new CheckMetaResult(
                            false,
                            "Failed to get table from StoreMaster, maybe still in replay after a truncate");
                } else if (e.getMessage().contains("canceling statement due to user request")) {
                    // 有些ERRORCODE可能并非PSQLState.QUERY_CANCELED,  加上根据message来判断
                    return new CheckMetaResult(
                            false,
                            "Get table from StoreMaster timeout, maybe store master is high load.");
                } else {
                    throw e;
                }
            }
        } else {
            refreshMeta(conn, timeout);
            return new CheckMetaResult(true, null);
        }
    }

    public static Tuple<HoloVersion, PgConnection> getHoloVersionByFixedFe(
            PgConnection conn, String jdbcUrl, Properties info) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try {
                String hgverStr = parseSingleCell(stmt.executeQuery("select hg_version()"));
                return new Tuple<>(parseHgVersion(hgverStr), conn);
            } catch (Exception e) {
                // ignore error as hg_version() does NOT exist in earlier hologres instances.
            }
        }
        // has exception
        // close conn
        if (null != conn) {
            try {
                conn.close();
                conn = null;
            } catch (Exception ignore) {
            }
        }

        // rebuild conn
        try {
            conn = DriverManager.getConnection(jdbcUrl, info).unwrap(PgConnection.class);
        } catch (Exception e) {
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception ignore) {
                }
            }
            throw e;
        }

        return new Tuple<>(new HoloVersion(1, 3, 1), conn);
    }

    public static HoloVersion getHoloVersion(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try {
                String hgverStr = parseSingleCell(stmt.executeQuery("select hg_version()"));
                return parseHgVersion(hgverStr);
            } catch (Exception e) {
                // ignore error as hg_version() does NOT exist in earlier hologres instances.
            }
        }
        try (Statement stmt = conn.createStatement()) {
            String verStr = parseSingleCell(stmt.executeQuery("select version()"));
            return parseHoloVersion(verStr);
        }
    }

    public static HoloVersion parseHgVersion(String verStr) throws Exception {
        // Hologres 0.11.1 (tag: release-0.11.x build: Release,Skylake,clang,no-coroutine)
        Matcher matcher = hgVersionPattern.matcher(verStr);
        if (matcher.find()) {
            String v = matcher.group(1);
            HoloVersion hv = new HoloVersion(v);
            if (!hv.isUndefined()) {
                return hv;
            }
        }
        throw new Exception("Failed to parse hg_version() result");
    }

    public static HoloVersion parseHoloVersion(String verStr) {
        // "PostgreSQL 11.3 (Release-build@4bf700d062 on origin/release-0.8.x) on
        // x86_64-pc-linux-gnu, compiled by x86_64-pc-linux-gnu-gcc (GCC) 8.3.0, 64-bit"
        Matcher matcher = holoVersionPattern.matcher(verStr);
        if (matcher.find()) {
            String v = matcher.group(1);
            return new HoloVersion(v);
        }
        return null;
    }

    public static String getDatabase(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            return parseSingleCell(stat.executeQuery("select current_database()"));
        }
    }

    public static Integer getBackendPid(Connection conn) {
        try (Statement stat = conn.createStatement()) {
            return Integer.parseInt(parseSingleCell(stat.executeQuery("select pg_backend_pid()")));
        } catch (SQLException ignored) {
            return -1;
        }
    }

    public static Long getFixedBackendPid(Connection conn) {
        try (Statement stat = conn.createStatement()) {
            return Long.parseLong(parseSingleCell(stat.executeQuery("select hg_fixed_conn_id()")));
        } catch (SQLException ignored) {
            return -1L;
        }
    }

    // SELECT n.nspname as Schema, c.relname as Name,
    // part.partstrat,
    // part.partnatts,
    // part.partattrs
    // FROM pg_catalog.pg_class c
    // JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    // JOIN pg_catalog.pg_partitioned_table part ON c.oid = part.partrelid
    // where n.nspname='public' and c.relname='test_message'
    // limit 1;
    //  schema |     name     | partstrat | partnatts | partattrs
    // --------+--------------+-----------+-----------+-----------
    //  public | test_message | l         |         1 | 1

    // SELECT attname
    // FROM pg_attribute
    // WHERE attrelid = 'public.test_message'::regclass
    // AND attnum = 1;
    // attname
    // ---------
    // ds
    public static String getPartitionColumnName(Connection conn, TableName tableName)
            throws SQLException {
        StringBuilder sb = new StringBuilder(512);
        sb.append("SELECT n.nspname as Schema, c.relname as Name,\n");
        sb.append("    part.partstrat,\n");
        sb.append("    part.partnatts,\n");
        sb.append("    part.partattrs\n");
        sb.append("FROM pg_catalog.pg_class c\n");
        sb.append("JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n");
        sb.append("JOIN pg_catalog.pg_partitioned_table part ON c.oid = part.partrelid\n");
        sb.append("where n.nspname=? and c.relname=? \n");
        sb.append("limit 1;\n");
        String sql = sb.toString();

        int partColumnPos = -1;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName.getSchemaName());
            stmt.setString(2, tableName.getTableName());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                String strategyStr = rs.getString("partstrat");
                if (!"l".equals(strategyStr)) {

                    throw new SQLException("Only LIST partition is supported in holo.");
                }

                String partColumnStr = rs.getString("partattrs");
                partColumnPos = Integer.parseInt(partColumnStr);
            }
        }

        String partColumnName = null;
        sql = "SELECT attname FROM pg_attribute WHERE attrelid =?::regclass AND attnum = ?;";
        if (partColumnPos > 0) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, tableName.getFullName());
                stmt.setInt(2, partColumnPos);
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    return rs.getString("attname");
                }
            }
        }
        return partColumnName;
    }

    // Query all child tables
    // with inh as (
    //     SELECT i.inhrelid, i.inhparent
    //     FROM pg_catalog.pg_class c
    //     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    //     LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent
    //     where n.nspname='public' and c.relname='test_message'
    // )
    // select
    //     n.nspname as schema_name,
    //     c.relname as table_name,
    //     inh.inhrelid, inh.inhparent, p.partstrat,
    //     pg_get_expr(c.relpartbound, c.oid, true) as part_expr,
    //     p.partdefid,
    //     p.partnatts,
    //     p.partattrs
    // from inh
    // join pg_catalog.pg_class c on inh.inhrelid = c.oid
    // join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    // join pg_partitioned_table p on p.partrelid = inh.inhparent;
    //
    //  schema_name |    table_name    | inhrelid | inhparent | partstrat |       part_expr       |
    // partdefid | partnatts | partattrs
    // -------------+------------------+----------+-----------+-----------+-----------------------+-----------+-----------+-----------
    //  public      | test_message_foo |    65655 |     65650 | l         | FOR VALUES IN ('foo') |
    //        0 |         1 | 1
    //  public      | test_message_bar |    65663 |     65650 | l         | FOR VALUES IN ('bar') |
    //        0 |         1 | 1
    // (2 rows)
    public static Partition getPartition(
            Connection conn, String schemaName, String tableName, String partValue, boolean isStr)
            throws SQLException {
        StringBuilder sb = new StringBuilder(512);
        sb.append("with inh as ( \n");
        sb.append("    SELECT i.inhrelid, i.inhparent \n");
        sb.append("    FROM pg_catalog.pg_class c \n");
        sb.append("    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \n");
        sb.append("    LEFT JOIN pg_catalog.pg_inherits i on c.oid=i.inhparent \n");
        sb.append("    where n.nspname=? and c.relname=? \n");
        sb.append(") \n");
        sb.append("select \n");
        sb.append("    n.nspname as schema_name, \n");
        sb.append("    c.relname as table_name, \n");
        sb.append("    inh.inhrelid, inh.inhparent, p.partstrat, \n");
        sb.append("    pg_get_expr(c.relpartbound, c.oid, true) as part_expr, \n");
        sb.append("    p.partdefid, \n");
        sb.append("    p.partnatts, \n");
        sb.append("    p.partattrs \n");
        sb.append("from inh \n");
        sb.append("join pg_catalog.pg_class c on inh.inhrelid = c.oid \n");
        sb.append("join pg_catalog.pg_namespace n on c.relnamespace = n.oid \n");
        sb.append(
                "join pg_partitioned_table p on p.partrelid = inh.inhparent where pg_get_expr(c.relpartbound, c.oid, true)=? limit 1 \n");

        String sql = sb.toString();
        ResultSet rs = null;
        Partition partition = null;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setString(
                    3,
                    "FOR VALUES IN (" + (isStr ? "'" : "") + partValue + (isStr ? "'" : "") + ")");
            rs = stmt.executeQuery();
            if (rs.next()) {

                String strategyStr = rs.getString("partstrat");
                if (!"l".equals(strategyStr)) {

                    throw new SQLException("Only LIST partition is supported in holo.");
                }
                partition = new Partition();
                partition.setParentSchemaName(schemaName);
                partition.setParentTableName(tableName);
                String schema = rs.getString("schema_name");
                String table = rs.getString("table_name");
                partition.setSchemaName(schema);
                partition.setTableName(table);
                partition.setPartitionValue(partValue);
            }

            return partition;
        }
    }

    public static Partition retryCreatePartitionChildTable(
            Connection conn,
            String schemaName,
            String tableName,
            String partValue,
            boolean isStr,
            String partSuffix)
            throws SQLException {

        int retry = 0;
        while (true) {
            Statement stmt = null;
            String childSchemaName = schemaName;
            String childTableName =
                    retry == 0
                            ? String.format("%s_%s", tableName, partSuffix)
                            : String.format(
                                    "%s_%s_%d", tableName, partSuffix, System.currentTimeMillis());

            try {
                String valueStr = null;
                if (isStr) {
                    valueStr = String.format("'%s'", partValue);
                } else {
                    valueStr = partValue;
                }
                String sql =
                        String.format(
                                "create table %s.%s partition of %s.%s for values in (%s);",
                                IdentifierUtil.quoteIdentifier(childSchemaName, true),
                                IdentifierUtil.quoteIdentifier(childTableName, true),
                                IdentifierUtil.quoteIdentifier(schemaName, true),
                                IdentifierUtil.quoteIdentifier(tableName, true),
                                valueStr);

                stmt = conn.createStatement();
                stmt.execute(sql);

                Partition partition = new Partition();
                partition.setTableName(childTableName);
                partition.setSchemaName(childSchemaName);
                partition.setParentTableName(tableName);
                partition.setParentSchemaName(schemaName);
                partition.setPartitionValue(valueStr);
                return partition;
            } catch (SQLException e) {
                String alreadyExistMsg =
                        String.format("relation \"%s\" already exists", childTableName);
                if (e.getMessage().indexOf(alreadyExistMsg) != -1 && retry < 20) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException ex) {
                    }
                    // retry
                    ++retry;
                    continue;
                }
                throw e;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }

    public static TableSchema getTableSchema(Connection conn, TableName tableName)
            throws SQLException {
        // PgDatabaseMetaData只会在判断服务端版本大于等于pg12时才返回正确的generated column信息, 这里我们先自己查系统表获取
        Set<String> generatedColumns = new HashSet<>();
        HoloVersion holoVersion = getHoloVersion(conn);
        if (holoVersion.compareTo(new HoloVersion(3, 1, 0)) >= 0) {
            StringBuilder sb = new StringBuilder(512);
            sb.append("SELECT\n");
            sb.append("    n.nspname,\n");
            sb.append("    c.relname,\n");
            sb.append("    a.attname,\n");
            sb.append("    nullif(a.attgenerated, '') as attgenerated\n");
            sb.append("FROM\n");
            sb.append("    pg_catalog.pg_namespace n\n");
            sb.append("    JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)\n");
            sb.append("    JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)\n");
            sb.append("WHERE\n");
            sb.append("    c.relkind IN ('r', 'p', 'v', 'f', 'm')\n");
            sb.append("    AND a.attnum > 0\n");
            sb.append("    AND NOT a.attisdropped\n");
            sb.append("    AND n.nspname = ?\n");
            sb.append("    AND c.relname = ?;");
            String generatedColumnInfoSql = sb.toString();
            try (PreparedStatement stat = conn.prepareStatement(generatedColumnInfoSql)) {
                stat.setString(1, tableName.getSchemaName());
                stat.setString(2, tableName.getTableName());
                try (ResultSet rs = stat.executeQuery()) {
                    while (rs.next()) {
                        String columnName = rs.getString(3);
                        String attgenerated = rs.getString(4);
                        if (attgenerated != null) {
                            generatedColumns.add(columnName);
                        }
                    }
                }
            }
        }

        DatabaseMetaData metaData = conn.getMetaData();
        List<String> primaryKeyList = new ArrayList<>();
        try (ResultSet rs =
                metaData.getPrimaryKeys(
                        null, tableName.getSchemaName(), tableName.getTableName())) {
            while (rs.next()) {
                primaryKeyList.add(rs.getString(4));
            }
        }
        List<Column> columnList = new ArrayList<>();
        String escape = metaData.getSearchStringEscape();
        // getColumns方法的后三个参数都是LIKE表达式使用的pattern，因此需要对这里传入的schemaName和tableName中的特殊字符进行转义。
        // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE
        // PG文档中明确LIKE的特殊字符只有%以及_（还有转义字符本身）。
        try (ResultSet rs =
                metaData.getColumns(
                        null,
                        escapePattern(tableName.getSchemaName(), escape),
                        escapePattern(tableName.getTableName(), escape),
                        "%")) {
            while (rs.next()) {
                Column column = new Column();
                column.setName(rs.getString(4));
                column.setType(rs.getInt(5));
                column.setTypeName(rs.getString(6));
                column.setPrecision(rs.getInt(7));
                column.setScale(rs.getInt(9));
                column.setAllowNull(rs.getInt(11) == 1);
                column.setComment(rs.getString(12));
                column.setDefaultValue(rs.getObject(13));
                column.setArrayType(column.getTypeName().startsWith("_"));
                if (column.isArrayType()) {
                    column.setArrayElementType(column.getTypeName().replaceFirst("_", ""));
                }
                column.setPrimaryKey(primaryKeyList.contains(column.getName()));
                // column.setGeneratedColumn(rs.getString(24).equals("YES"));
                column.setGeneratedColumn(generatedColumns.contains(column.getName()));
                columnList.add(column);
            }
        }

        String partitionColumnName = getPartitionColumnName(conn, tableName);

        String sql =
                "select property_key,property_value from hologres.hg_table_properties where table_namespace=? and table_name=? and property_key in "
                        + "('distribution_key','table_id','schema_version','orientation','clustering_key','segment_key','bitmap_columns','dictionary_encoding_columns','time_to_live_in_seconds','binlog.level', 'is_logical_partitioned_table')";
        String[] distributionKeys = null;
        String tableId = null;
        String schemaVersion = null;
        Map<String, String> properties = new HashMap<>();
        try (PreparedStatement stat = conn.prepareStatement(sql)) {
            stat.setString(1, tableName.getSchemaName());
            stat.setString(2, tableName.getTableName());

            try (ResultSet rs = stat.executeQuery()) {
                while (rs.next()) {
                    String propertyName = rs.getString(1);
                    String propertyValue = rs.getString(2);
                    properties.put(propertyName, propertyValue);
                }
            }
            tableId = properties.get("table_id");
            schemaVersion = properties.get("schema_version");
        }
        if (properties.size() == 0) {
            throw new SQLException("can not found table " + tableName.getFullName());
        }
        if (tableId == null) {
            throw new SQLException("table " + tableName.getFullName() + " has no table_id");
        }
        if (schemaVersion == null) {
            throw new SQLException("table " + tableName.getFullName() + " has no schemaVersion");
        }
        TableSchema.Builder builder = new TableSchema.Builder(tableId, schemaVersion);

        builder.setPartitionColumnName(partitionColumnName);
        builder.setColumns(columnList);
        builder.setTableName(tableName);
        builder.setNotExist(false);
        builder.setSensitive(true);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            switch (key) {
                case "distribution_key":
                    if ("".equals(value)) {
                        throw new SQLException("empty distribution_key is not supported.");
                    }
                    builder.setDistributionKeys(value.split(","));
                    break;
                case "orientation":
                    builder.setOrientation(value);
                    break;
                case "clustering_key":
                    builder.setClusteringKey(value.split(","));
                    break;
                case "segment_key":
                    builder.setSegmentKey(value.split(","));
                    break;
                case "bitmap_columns":
                    builder.setBitmapIndexKey(value.split(","));
                    break;
                case "dictionary_encoding_columns":
                    builder.setDictionaryEncoding(value.split(","));
                    break;
                case "time_to_live_in_seconds":
                    builder.setLifecycle(Long.parseLong(value));
                    break;
                case "binlog.level":
                    builder.setBinlogLevel(value);
                    break;
                case "is_logical_partitioned_table":
                    builder.setIsLogicalPartitionedTable(Boolean.parseBoolean(value));
                    break;
                default:
            }
        }
        if (partitionColumnName != null) {
            boolean supportTimeFormat = holoVersion.compareTo(new HoloVersion(3, 0, 12)) >= 0;
            if (supportTimeFormat) {
                sql =
                        "SELECT time_unit,time_zone,num_precreate,time_format from hologres.hg_partitioning_config where enable = true and nsp_name=? and tbl_name=?";
            } else {
                sql =
                        "SELECT time_unit,time_zone,num_precreate from hologres.hg_partitioning_config where enable = true and nsp_name=? and tbl_name=?";
            }
            String pgTimeZoneName = null;
            try (PreparedStatement stat = conn.prepareStatement(sql)) {
                stat.setString(1, tableName.getSchemaName());
                stat.setString(2, tableName.getTableName());
                try (ResultSet rs = stat.executeQuery()) {
                    while (rs.next()) {
                        builder.setAutoPartitioningEnable(true);
                        builder.setAutoPartitioningTimeUnit(rs.getString("time_unit"));
                        pgTimeZoneName = rs.getString("time_zone");
                        builder.setAutoPartitioningPreCreateNum(rs.getInt("num_precreate"));
                        if (supportTimeFormat) {
                            builder.setAutoPartitioningTimeFormat(rs.getString("time_format"));
                        }
                    }
                }
            } catch (SQLException e) {
                if (e.getMessage()
                        .contains("relation \"hologres.hg_partitioning_config\" does not exist")) {
                    builder.setAutoPartitioningEnable(false);
                } else {
                    throw e;
                }
            }
            if (pgTimeZoneName != null && !builder.setAutoPartitioningTimeZone(pgTimeZoneName)) {
                boolean autoCommit = conn.getAutoCommit();
                try (Statement stmt = conn.createStatement()) {
                    conn.setAutoCommit(false);
                    stmt.execute("BEGIN;");
                    stmt.execute(String.format("set local timezone = '%s'", pgTimeZoneName));
                    // https://www.postgresql.org/docs/17/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT, extract(timezone from now())
                    try (ResultSet rs =
                            stmt.executeQuery(
                                    "SELECT EXTRACT(TIMEZONE FROM NOW()) AS timezone_offset_seconds;")) {
                        if (rs.next()) {
                            builder.setAutoPartitioningTimeZoneByOffset(
                                    rs.getInt("timezone_offset_seconds"));
                        }
                    }
                } finally {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute("END;");
                    } catch (SQLException ignored) {
                    }
                    conn.setAutoCommit(autoCommit);
                }
            }
        }
        TableSchema tableSchema = builder.build();
        tableSchema.calculateProperties();
        return tableSchema;
    }

    private static String parseSingleCell(ResultSet rs) throws SQLException {
        String ret = null;
        if (rs.next()) {
            ret = rs.getString(1);
        }
        return ret;
    }

    private static String escapePattern(String pattern, String escape) {
        return pattern.replace(escape, escape + escape)
                .replace("%", escape + "%")
                .replace("_", escape + "_");
    }

    /**
     * 返回直连fe或者fixedfe的jdbc url，如果网络连通可以不走vip.
     *
     * <p>获取到直连url后会立刻尝试直连一次，无法直连回退到使用vip.
     *
     * @return 直连fe的jdbc url
     * @throws SQLException
     */
    public static String getDirectConnectionUrl(
            String originalJdbcUrl, Properties info, boolean isFixed) throws SQLException {
        LOGGER.info(
                "Try to connect {} for getting fe endpoint, isFixedFe {}",
                originalJdbcUrl,
                isFixed);
        String addr = null;
        String port = null;
        // 由于消费binlog的info中可能设置REPLICATION参数，无法支持select，因此新建Properties传入username和password创建连接
        Properties tempInfo = new java.util.Properties();
        PGProperty.USER.set(tempInfo, info.getProperty(PGProperty.USER.getName()));
        PGProperty.PASSWORD.set(tempInfo, info.getProperty(PGProperty.PASSWORD.getName()));
        if (info.getProperty(PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.getName()) != null) {
            PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.set(
                    tempInfo,
                    info.getProperty(PGProperty.AUTHENTICATION_PLUGIN_CLASS_NAME.getName()));
        }
        if (info.getProperty(AKv4AuthenticationPlugin.REGION) != null) {
            tempInfo.setProperty(
                    AKv4AuthenticationPlugin.REGION,
                    info.getProperty(AKv4AuthenticationPlugin.REGION));
        }
        try (PgConnection conn =
                DriverManager.getConnection(originalJdbcUrl, tempInfo).unwrap(PgConnection.class)) {
            try (Statement stat = conn.createStatement()) {
                try (ResultSet rs =
                        stat.executeQuery("select inet_server_addr(), inet_server_port()")) {
                    while (rs.next()) {
                        addr = rs.getString(1);
                        port = rs.getString(2);
                    }
                }
                if (isFixed) {
                    try (ResultSet rs = stat.executeQuery("show fixed_frontend_port")) {
                        while (rs.next()) {
                            port = rs.getString(1);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.warn("Get the direct connection jdbc url failed, use original url.", e);
            return originalJdbcUrl;
        }
        if (addr == null || port == null) {
            LOGGER.warn("Get the direct connection jdbc url failed, use original url.");
            return originalJdbcUrl;
        }
        String directConnectionJdbcUrl =
                ConnectionUtil.replaceJdbcUrlEndpoint(originalJdbcUrl, addr + ":" + port);
        // 调整连接超时为1s, 快速验证当前环境是否支持直连, 不支持回退到使用vip
        PGProperty.CONNECT_TIMEOUT.set(tempInfo, 1);
        try (PgConnection ignored =
                DriverManager.getConnection(directConnectionJdbcUrl, tempInfo)
                        .unwrap(PgConnection.class)) {
        } catch (SQLException e) {
            LOGGER.warn(
                    "Could not direct connect {} because \"{}\", use original url.",
                    directConnectionJdbcUrl,
                    e.getMessage());
            LOGGER.debug("Could not direct connect", e);
            return originalJdbcUrl;
        }
        LOGGER.info(
                "Get the direct connection {} jdbc url {}",
                isFixed ? "fixed fe" : "fe",
                directConnectionJdbcUrl);
        return directConnectionJdbcUrl;
    }

    public static String replaceJdbcUrlEndpoint(String originalUrl, String newEndpoint) {
        String replacement = "//" + newEndpoint + "/";
        return originalUrl.replaceFirst("//\\S+/", replacement);
    }

    public static String generateFixedUrl(String url) {
        StringBuilder sb = new StringBuilder(url);
        int index = sb.lastIndexOf(optionProperty);
        if (index > -1) {
            // In fact fixed fe will ignore url parameters, but we guarantee the correctness of the
            // url here.
            sb.insert(index + optionProperty.length(), fixedOption);
        } else {
            sb.append(url.contains("?") ? "&" : "?").append(optionProperty).append(fixedOption);
        }
        return sb.toString();
    }

    public static String getQueryIdFromNotice(SQLWarning warning) {
        while (warning != null) {
            String message = warning.getMessage();
            if (message != null && message.contains("QueryID")) {
                return message.replace("QueryID: ", "");
            }
            warning = warning.getNextWarning();
        }
        return "";
    }

    public static final String DEFAULT_PUB_NAME = "publication_for_table_%s_used_by_holo_client";
    public static final String DEFAULT_SLOT_NAME = "slot_for_table_%s_used_by_holo_client";

    public static String getOrCreateDefaultSlot(Connection conn, TableName tableName) {
        String pubName = String.format(DEFAULT_PUB_NAME, formatTableName(tableName.getFullName()));
        String slotName =
                String.format(DEFAULT_SLOT_NAME, formatTableName(tableName.getFullName()));
        try (Statement statement = conn.createStatement()) {
            statement.execute("create extension if not exists hg_binlog;");
            try {
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "select * from pg_publication where pubname='%s';",
                                        pubName));
                if (rs.next()) {
                    LOGGER.warn(
                            "drop the publication \"{}\" because it may be the remaining publication before table re create.",
                            pubName);
                    statement.execute(String.format("drop publication \"%s\";", pubName));
                }
            } catch (SQLException ignored) {
            }
            try {
                statement.execute(
                        String.format(
                                "create publication \"%s\" for table %s;",
                                pubName, tableName.getFullName()));
                LOGGER.info(
                        "create a default publication \"{}\" for table {}.",
                        pubName,
                        tableName.getFullName());
            } catch (SQLException e) {
                if (!e.getMessage()
                        .contains(String.format("publication \"%s\" already exists", pubName))) {
                    throw new RuntimeException(e);
                }
            }
            try {
                statement.execute(
                        String.format("call hg_drop_logical_replication_slot('%s');", slotName));
                statement.execute(
                        String.format(
                                "call hg_create_logical_replication_slot('%s', 'hgoutput', '%s');",
                                slotName, pubName));
                LOGGER.info(
                        "create a default replication slot \"{}\" for table {} in pub \"{}\".",
                        slotName,
                        tableName.getFullName(),
                        pubName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return slotName;
        } catch (SQLException e) {
            throw new RuntimeException("get or create default slot for JDBC binlog failed", e);
        }
    }

    /**
     * The slot name processing logic in hologres is not perfect for uppercase and special
     * characters, so we ensure that the slot name only has words, numbers and underscores.
     */
    private static String formatTableName(String tableName) {
        // remove quotes and replace non-alphanumeric characters with _
        // The hash is to solve: when table name is converted to lowercase, two table name could
        // same.
        return tableName.replaceAll("\"", "").replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase()
                + (Objects.hash(tableName) & Integer.MAX_VALUE);
    }

    public static Partition getOrCreatePartition(
            Connection conn,
            TableName tableName,
            String value,
            boolean isStr,
            boolean createIfNotExists,
            String partSuffix)
            throws SQLException {
        Partition internalPartition =
                getPartition(
                        conn, tableName.getSchemaName(), tableName.getTableName(), value, isStr);
        if (internalPartition == null) {
            if (!createIfNotExists) {
                return null;
            } else {
                try {
                    internalPartition =
                            retryCreatePartitionChildTable(
                                    conn,
                                    tableName.getSchemaName(),
                                    tableName.getTableName(),
                                    value,
                                    isStr,
                                    partSuffix);
                } catch (SQLException e) {
                    internalPartition =
                            getPartition(
                                    conn,
                                    tableName.getSchemaName(),
                                    tableName.getTableName(),
                                    value,
                                    isStr);
                    if (internalPartition == null) {
                        throw new SQLException(
                                "after create, partition child table is still not exists, tableName:"
                                        + tableName.getFullName()
                                        + ",partitionValue:"
                                        + value);
                    }
                }
                return internalPartition;
            }
        } else {
            return internalPartition;
        }
    }
}
