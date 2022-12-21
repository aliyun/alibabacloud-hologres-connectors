package com.alibaba.hologres.shipper.holo;

import com.alibaba.hologres.client.*;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.ImportContext;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.org.postgresql.util.HoloVersion;
import com.alibaba.hologres.org.postgresql.util.MetaUtil;
import com.alibaba.hologres.shipper.HoloDBShipper;
import com.alibaba.hologres.shipper.generic.AbstractTable;
import org.postgresql.model.TableSchema;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HoloTable extends AbstractTable {
    public static final Logger LOGGER = LoggerFactory.getLogger(HoloTable.class);
    String tableName;
    String schemaName;
    String pureTableName;
    HoloClient client;
    HoloVersion version;

    public HoloTable(String tableName, String jdbcUrl, String user, String password, HoloVersion version) throws Exception{
        this.tableName = HoloUtils.getTableNameWithQuotes(tableName);
        this.schemaName = tableName.split("\\.",2)[0];
        this.pureTableName = tableName.split("\\.",2)[1];
        try {
            if(version != null && version.getMajorVersion() == 0 && version.getMinorVersion() <= 5)
                this.client = HoloUtils.getSimpleClient(jdbcUrl, user, password);
            else
                this.client = HoloUtils.getHoloClient(jdbcUrl, user, password, HoloDBShipper.NUM_BATCH);
            this.version  = (HoloVersion)(client.sql(MetaUtil::getHoloVersion)).get();
        }catch(HoloClientException e) {
            LOGGER.error("Failed creating holo-client for table " + tableName, e);
            throw e;
        }
    }

    public String getTableDDL(boolean hasToolkit) {
        if(!hasToolkit) {
            return getTableDDLManually();
        }
        LOGGER.info("Starting fetching DDL for table "+tableName);
        String DDLInfo = null;
        try {
            DDLInfo = (String) client.sql(conn -> {
                String ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(String.format("SELECT hg_dump_script('%s')", tableName))) {
                        rs.next();
                        ret = rs.getString("hg_dump_script");
                    }
                }
                return ret;
            }).get();
            LOGGER.info("Finished fetching DDL for table "+tableName);
        } catch (Exception e) {
            LOGGER.error("Failed fetching DDL for table "+tableName, e);
        }
        return DDLInfo;
    }

    public String getTableDDLManually() {
        LOGGER.info("Starting fetching DDL manually for table " + tableName);
        //create table
        String tableDDL = null;
        try{
            String findOidSQl = String.format("SELECT c.oid,\n" +
                    "  n.nspname,\n" +
                    "  c.relname\n" +
                    "FROM pg_catalog.pg_class c\n" +
                    "     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" +
                    "WHERE c.relname = '%s'\n" +
                    "  AND n.nspname = '%s'\n" +
                    "ORDER BY 2, 3", pureTableName, schemaName);
            int oid = client.sql(conn -> {
                int ret;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findOidSQl)) {
                        rs.next();
                        ret = rs.getInt("oid");
                    }
                }
                return ret;
            }).get();
            String findParentSql = String.format("SELECT inhparent::pg_catalog.regclass,\n" +
                    "  pg_catalog.pg_get_expr(c.relpartbound, inhrelid)\n" +
                    "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON c.oid = inhrelid\n" +
                    "WHERE c.oid = '%d' AND c.relispartition", oid);
            String parentInfo = client.sql(conn -> {
                String ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findParentSql)) {
                        if(rs.next()) {
                            String parent = rs.getString("inhparent");
                            String value = rs.getString("pg_get_expr");
                            ret = "PARTITION OF " + parent + ' ' + value;
                        }
                    }
                }
                return ret;
            }).get();
            String findColumnsSql = String.format("select column_name, data_type, is_nullable, character_maximum_length from information_schema.columns " +
                    "where table_schema = '%s' and table_name = '%s'", schemaName, pureTableName);
            String columnInfo = client.sql(conn -> {
                String ret = "(\n";
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findColumnsSql)) {
                        while(rs.next()) {
                            ret = ret + "    " + rs.getString("column_name") + " " + rs.getString("data_type");
                            int maxLength = rs.getInt("character_maximum_length");
                            if(maxLength > 0)
                                ret += String.format("(%d)", maxLength);
                            if(rs.getString("is_nullable").equals("NO"))
                                ret += " NOT NULL";
                            ret += ",\n";
                        }
                    }
                    ret = ret.substring(0, ret.length()-2);
                    String findPrimaryKeySql = String.format("SELECT conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef\n" +
                            "FROM pg_catalog.pg_constraint r\n" +
                            "WHERE r.conrelid = '%d' AND r.contype = 'p' ORDER BY 1", oid);
                    try (ResultSet rs = stmt.executeQuery(findPrimaryKeySql)) {
                        if(rs.next()) {
                            String primaryKey = rs.getString("condef");
                            if (primaryKey != null)
                                ret = ret + "\n    ," + primaryKey;
                        }
                    }
                }
                ret += "\n)";
                return ret;
            }).get();
            String findPartitionSql = String.format("SELECT pg_catalog.pg_get_partkeydef('%d'::pg_catalog.oid)", oid);
            String partitionInfo = client.sql(conn -> {
                String ret = null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findPartitionSql)) {
                        if(rs.next() && rs.getString("pg_get_partkeydef") != null) {
                            ret = "\nPARTITION BY " + rs.getString("pg_get_partkeydef");
                        }
                    }
                }
                return ret;
            }).get();
            String createTable = String.format("CREATE TABLE %s.%s ", schemaName, pureTableName);
            if(parentInfo != null)
                createTable = createTable + parentInfo + ";\n";
            else {
                createTable += columnInfo;
                if(partitionInfo != null)
                    createTable = createTable + partitionInfo;
                createTable += ";\n";
            }
            //table properties
            String findPropertiesSQl1 = String.format("select property_key, property_value from hologres.holo_table_properties where table_namespace = '%s' and table_name = '%s'", schemaName, pureTableName);
            String findPropertiesSQl2 = String.format("select property_key, property_value from hologres.hg_table_properties where table_namespace = '%s' and table_name = '%s'", schemaName, pureTableName);
            String tableProperties = client.sql(conn -> {
                String ret = "";
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = null;
                    try{
                        rs = stmt.executeQuery(findPropertiesSQl1);
                    } catch (SQLException e) {
                        rs = stmt.executeQuery(findPropertiesSQl2);
                    }
                    List<String> possibleProperties = Arrays.asList("orientation", "clustering_key", "segment_key", "bitmap_columns", "dictionary_encoding_columns", "distribution_key", "time_to_live_in_seconds", "storage_format");
                    while(rs.next()) {
                        String propertyKey = rs.getString("property_key");
                        if(possibleProperties.contains(propertyKey))
                            ret += String.format("CALL set_table_property('%s', '%s', '%s');\n", tableName, propertyKey, rs.getString("property_value"));
                    }
                    rs.close();
                }
                return ret;
            }).get();
            String findTableCommentSql = String.format("SELECT obj_description(%d)", oid);
            String tableComment = client.sql(conn -> {
                String comment = "NULL";
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findTableCommentSql)) {
                        if(rs.next() && rs.getString("obj_description") != null) {
                            comment = String.format("'%s'", rs.getString("obj_description"));
                        }
                    }
                }
                String ret = String.format("COMMENT ON TABLE %s IS %s;\n", tableName, comment);
                return ret;
            }).get();
            String findOwnerSql = String.format("SELECT tableowner FROM pg_catalog.pg_tables where schemaname = '%s' and tablename = '%s'", schemaName, pureTableName);
            String tableOwner = client.sql(conn -> {
                String owner= null;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findOwnerSql)) {
                        rs.next();
                        owner = rs.getString("tableowner");
                    }
                }
                String ret = String.format("ALTER TABLE %s OWNER TO \"%s\";\n", tableName, owner);
                return ret;
            }).get();
            String findColCommentsSql = String.format("select column_name, col_description('%d', ordinal_position)\n" +
                    "from information_schema.columns\n" +
                    "where table_schema = '%s' and table_name = '%s'", oid, schemaName, pureTableName);
            String columnComments = client.sql(conn -> {
                String colComments = "";
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(findColCommentsSql)) {
                        while(rs.next()) {
                            String comment = rs.getString("col_description");
                            if(comment != null)
                                colComments += String.format("COMMENT ON COLUMN %s.\"%s\" IS '%s';\n", tableName, rs.getString("column_name"), comment);
                        }
                    }
                }
                return colComments;
            }).get();
            if(parentInfo != null)
                tableDDL = "BEGIN;\n\n" + createTable + '\n' + tableComment + tableOwner + columnComments + "\nEND;\n";
            else
                tableDDL = "BEGIN;\n\n" + createTable + '\n' + tableProperties + '\n' + tableComment + tableOwner + columnComments + "\nEND;\n";
        } catch (Exception e) {
            LOGGER.error("Failed fetching DDL for table " + tableName, e);
        }
        return tableDDL;
    }

    public void setTableDDL(String DDLInfo) throws Exception {
        try {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(DDLInfo);
                }
                return null;
            }).get();
        } catch (Exception e) {
            LOGGER.error("Failed setting DDL for table "+tableName, e);
            throw e;
        }
    }


    public String getPrivileges() {
        LOGGER.info("Starting fetching privileges info for " + tableName);
        String findPrivSql = String.format("SELECT grantee, STRING_AGG(privilege_type, ', ') AS privileges\n" +
                                           "FROM information_schema.table_privileges\n" +
                                           "WHERE table_schema = '%s' AND table_name = '%s'\n" +
                                           "GROUP BY grantee", schemaName, pureTableName);
        String privInfo = null;
        try {
            privInfo = (String) client.sql(conn -> {
                String priv = "BEGIN;\n";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(findPrivSql)) {
                    while(rs.next()) {
                        String grantee = rs.getString("grantee");
                        String privileges = rs.getString("privileges");
                        if(!grantee.equals("PUBLIC"))
                            grantee = "\"" + grantee + "\"";
                        priv = priv + String.format("GRANT %s ON %s TO %s;\n", privileges, tableName, grantee);
                    }
                }
                priv = priv + "END;\n";
                return priv;
            }).get();
            LOGGER.info("Finished fetching privileges info for " + tableName);
        } catch (Exception e)
        {
            LOGGER.error("Failed fetching privileges info for " + tableName, e);
        }

        return privInfo;
    }

    public void setPrivileges(String privInfo) {
        if(privInfo == null) {
            LOGGER.info("No privileges info");
            return;
        }
        try {
            client.sql(conn -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(privInfo);
                }
                return null;
            }).get();
        } catch (Exception e) {
            LOGGER.error("Failed setting privileges for table "+tableName, e);
        }
    }

    public void scanTableData(final PipedOutputStream os) {
        LOGGER.info("Starting scanning data of table "+tableName);
        int THRESHOLD = 100000;
        try (final PrintStream ps = new PrintStream(os)){
            TableSchema tableSchema = client.getTableSchema(tableName);
            int rowCount = client.sql(conn -> {
                int ret;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(String.format("select count(1) from %s", tableName))) {
                        rs.next();
                        ret = rs.getInt("count");
                    }
                }
                return ret;
            }).get();
            if(rowCount < THRESHOLD) { //small table, scan directly
                Scan scan = Scan.newBuilder(tableSchema).build();
                try (RecordScanner rs = client.scan(scan)) {
                    while (rs.next()) {
                        Record record = rs.getRecord();
                        ps.println(Arrays.toString(record.getValues()).replace("[", "").replace("]", "").replace("null", "N").trim());
                    }
                }
                LOGGER.info("Finished scanning data of table "+tableName);
                return;
            }
            //try to scan by shard
            int shardCount = client.sql(conn -> {
                int ret;
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(String.format("select max(hg_shard_id) from %s", tableName))) {
                        rs.next();
                        ret = rs.getInt("max");
                    }
                }
                return ret;
            }).get();
            for (int shard = 0; shard <= shardCount; shard++) {
                int finalShard = shard;
                client.sql(conn -> {
                    try (Statement stmt = conn.createStatement()) {
                        try (ResultSet rs = stmt.executeQuery(String.format("select * from %s where hg_shard_id = %d", tableName, finalShard))) {
                            int columnCount = rs.getMetaData().getColumnCount();
                            while(rs.next()) {
                                for(int i=1; i<columnCount; i++) {
                                    String value = rs.getString(i);
                                    value = value == null? "N":value;
                                    ps.print(value);
                                    ps.print(",");
                                }
                                String value = rs.getString(columnCount);
                                value = value == null? "N":value;
                                ps.print(value);
                                ps.print("\n");
                            }
                        }
                    }
                    return null;
                }).get();
                LOGGER.info(String.format("scan table %s shard %d finished", tableName, shard));
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Scan data in table %s failed.", tableName), e);
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                LOGGER.error("Failed closing PipedOutputStream", e);
            }
        }

    }

    public void readTableData(PipedOutputStream os, int startShard, int endShard) {
        if(version.getMajorVersion() == 0 && version.getMinorVersion() <= 5) {
            scanTableData(os);
            return;
        }
        try {
            TableSchema tableSchema = client.getTableSchema(tableName);
            ExportContext exportContext;
            if(startShard == -1)
                exportContext = client.exportData(Exporter.newBuilder(tableSchema).setOutputStream(os).build());
            else
                exportContext = client.exportData(Exporter.newBuilder(tableSchema).setShardRange(startShard, endShard).setOutputStream(os).build());
            while (!exportContext.getRowCount().isDone()) {
                Thread.sleep(5000L);
            }
            //throw if has exception
            exportContext.getRowCount().get();
            if(startShard == -1)
                LOGGER.info(String.format("Read table %s done", tableName));
            else
                LOGGER.info(String.format("Read table %s from shard %d to %d done", tableName, startShard, endShard));
        } catch (Exception e) {
            LOGGER.error("Failed reading data from table " + tableName, e);
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                LOGGER.error("Failed closing PipedOutputStream", e);
            }
        }
    }

    public void writeTableData(PipedInputStream is, int startShard, int endShard) {
        try {
            TableSchema tableSchema = client.getTableSchema(tableName);
            ImportContext importContext;
            if(startShard == -1)
                importContext = client.importData(Importer.newBuilder(tableSchema).setInputStream(is).build());
            else
                importContext = client.importData(Importer.newBuilder(tableSchema).setShardRange(startShard, endShard).setInputStream(is).build());
            while (!importContext.getRowCount().isDone()) {
                Thread.sleep(5000L);
            }
            //throw if has exception
            importContext.getRowCount().get();
            if(startShard == -1)
                LOGGER.info(String.format("Write table %s done", tableName));
            else
                LOGGER.info(String.format("Write table %s from shard %d to %d done", tableName, startShard, endShard));
        } catch (Exception e) {
            LOGGER.error("Failed writing data to table " + tableName, e);
        } finally {
            try {
                is.close();
            }catch(IOException e) {
                LOGGER.error("Failed closing PipedInputStream", e);
            }
        }
    }

    public int getShardCount() {
        if(version != null && version.getMajorVersion() == 0 && version.getMinorVersion() <= 5) {
            return 0; //不分shard
        }
        try {
            TableSchema tableSchema = client.getTableSchema(tableName);
            int shardCount = Command.getShardCount(client, tableSchema);
            return shardCount;
        } catch (HoloClientException e) {
            LOGGER.error("Failed reading shard count of table " + tableName, e);
            return 0;
        }
    }

    public Map<Integer, Integer> getBatches(int numBatch, int dstShardCount, boolean disableShardCopy) {
        Map<Integer, Integer> batches = new HashMap<>();
        int shardCount = this.getShardCount();
        if(shardCount == 0 || (shardCount != dstShardCount && dstShardCount > 0) || disableShardCopy) {
            batches.put(-1, -1); //不分shard
            return batches;
        }

        numBatch = Math.min(numBatch, shardCount);
        int batchLength = (int) Math.ceil((double)shardCount/(double)numBatch);
        int startShard = 0;
        while(startShard < shardCount) {
            int endShard = Math.min(startShard + batchLength, shardCount);
            batches.put(startShard, endShard);
            startShard = endShard;
        }
        return batches;
    }

    @Override
    public void close() {
        if(client != null) {
            client.close();
            client = null;
        }
    }
}