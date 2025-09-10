package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.binlog.BinlogLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;

/** TableSchema Tester. */
public class TableSchemaTest extends HoloClientTestBase {

    /** 无引号，有大写, 无特殊字符. Method: valueOf(String name). */
    @Test
    public void testBinlogLevel() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteThreadSize(10);
        config.setBinlogReadBatchSize(20);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName1 = "holo_client_open_binlog";
            String tableName2 = "holo_client_not_open_binlog";

            String dropSql1 = "drop table if exists " + tableName1;
            String dropSql2 = "drop table if exists " + tableName2;

            String createSql1 =
                    "create table "
                            + tableName1
                            + "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
                            + "call set_table_property('"
                            + tableName1
                            + "', 'binlog.level', 'replica');\n";

            String createSql2 =
                    "create table "
                            + tableName2
                            + "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n ";
            execute(
                    conn,
                    new String[] {
                        dropSql1,
                        dropSql2,
                        "begin;",
                        createSql1,
                        "commit;",
                        "begin;",
                        createSql2,
                        "commit;"
                    });

            try {
                TableSchema schema1 = client.getTableSchema(tableName1, true);
                Assert.assertEquals(schema1.getBinlogLevel(), BinlogLevel.REPLICA);
                TableSchema schema2 = client.getTableSchema(tableName2, true);
                Assert.assertEquals(schema2.getBinlogLevel(), BinlogLevel.NONE);
            } finally {
                execute(conn, new String[] {dropSql1, dropSql2});
            }
        }
    }

    @Test
    public void testTableSchemaToString() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteThreadSize(10);
        config.setBinlogReadBatchSize(20);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_table_schema_to_string";

            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null, amount decimal(12,2), t text not null, ts timestamptz not null, ba bytea, t_a text[],i_a int[], primary key(id));\n "
                            + "call set_table_property('"
                            + tableName
                            + "', 'binlog.level', 'replica');\n"
                            + "call set_table_property('"
                            + tableName
                            + "', 'clustering_key', 't');\n"
                            + "call set_table_property('"
                            + tableName
                            + "', 'orientation', 'column,row');\n"
                            + "call set_table_property('"
                            + tableName
                            + "', 'segment_key', 'ts');\n";
            execute(conn, new String[] {dropSql, "begin;", createSql, "commit;"});

            try {
                TableSchema schema = client.getTableSchema(tableName, true);
                Assert.assertEquals(
                        schema.toString(),
                        String.format(
                                "TableSchema{\n"
                                        + "tableId='%s', \n"
                                        + "schemaVersion='%S', \n"
                                        + "tableName=\"public\".\"%s\", \n"
                                        + "distributionKeys=[id], \n"
                                        + "clusteringKey=[t:asc], \n"
                                        + "segmentKey=[ts], \n"
                                        + "partitionInfo='null', \n"
                                        + "orientation='column,row', \n"
                                        + "binlogLevel=REPLICA, \n"
                                        + "columns=[\n"
                                        + "Column{name='id', typeName='int4', not null, primary key}, \n"
                                        + "Column{name='amount', typeName='numeric'}, \n"
                                        + "Column{name='t', typeName='text', not null}, \n"
                                        + "Column{name='ts', typeName='timestamptz', not null}, \n"
                                        + "Column{name='ba', typeName='bytea'}, \n"
                                        + "Column{name='t_a', typeName='_text'}, \n"
                                        + "Column{name='i_a', typeName='_int4'}]}",
                                schema.getTableId(), schema.getSchemaVersion(), tableName));
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
