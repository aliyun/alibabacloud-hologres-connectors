package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.binlog.BinlogLevel;
import com.alibaba.hologres.client.utils.IdentifierUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

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
                Assert.assertNull(schema.getGlobalIndexs());
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

    /** 测试 TableSchema 的序列化和反序列化，确保包含 GlobalIndex 的 TableSchema 可以正常序列化. */
    @Test
    public void testTableSchemaSerializable() throws Exception {
        Column col = new Column();
        col.setName("id");
        col.setType(4);
        col.setTypeName("int4");
        col.setPrimaryKey(true);
        col.setAllowNull(false);

        GlobalIndex globalIndex =
                new GlobalIndex(
                        TableName.valueOf("public.test_index"), new String[] {"col1", "col2"});

        TableSchema schema =
                new TableSchema.Builder("test_id", "v1")
                        .setTableName(TableName.valueOf("public.test_table"))
                        .addColumn(col)
                        .setDistributionKeys(new String[] {"id"})
                        .setGlobalIndexs(new GlobalIndex[] {globalIndex})
                        .build();
        schema.calculateProperties();

        // 序列化
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(schema);
        oos.close();

        // 反序列化
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        TableSchema deserialized = (TableSchema) ois.readObject();
        ois.close();

        // 验证反序列化结果
        Assert.assertEquals(deserialized.getTableId(), "test_id");
        Assert.assertEquals(deserialized.getSchemaVersion(), "v1");
        Assert.assertNotNull(deserialized.getGlobalIndexs());
        Assert.assertEquals(deserialized.getGlobalIndexs().length, 1);
        Assert.assertEquals(
                deserialized.getGlobalIndexs()[0].getIndexKeys(), new String[] {"col1", "col2"});
        Assert.assertEquals(deserialized.getColumnSchema().length, 1);
        Assert.assertEquals(deserialized.getColumnSchema()[0].getName(), "id");
    }

    /**
     * 确保 TableSchema 的所有非静态非transient成员字段的类型都实现了 Serializable， 防止后续新增字段忘记实现 Serializable 导致线上问题.
     */
    @Test
    public void testAllFieldsAreSerializable() {
        List<String> nonSerializableFields = new ArrayList<>();
        for (Field field : TableSchema.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                    || Modifier.isTransient(field.getModifiers())) {
                continue;
            }
            Class<?> fieldType = field.getType();
            // 基本类型天然可序列化
            if (fieldType.isPrimitive()) {
                continue;
            }
            // 接口和抽象类(如Map、Set)跳过声明类型检查，由实际序列化测试保障
            if (fieldType.isInterface() || Modifier.isAbstract(fieldType.getModifiers())) {
                continue;
            }
            if (!Serializable.class.isAssignableFrom(fieldType)) {
                nonSerializableFields.add(field.getName() + " (type: " + fieldType.getName() + ")");
            }
        }
        Assert.assertTrue(
                nonSerializableFields.isEmpty(),
                "TableSchema contains non-Serializable fields: " + nonSerializableFields);
    }

    @Test
    public void testTableSchemaWithGlobalIndex() throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        config.setWriteThreadSize(10);

        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String schemaName = genRandomStr(6);
            String tableName = "holo_client_table_schema_with_global_index_" + genRandomStr(6);
            String indexName1 = "idx1_" + genRandomStr(6);
            String indexName2 = "idx2_" + genRandomStr(6);

            String colName1 = "col1" + genRandomStr(6);
            String colName2 = "col2";
            String colName3 = "col3";

            String dropSql =
                    "drop table if exists "
                            + TableName.quoteValueOf(schemaName, tableName).getFullName();
            String createSchemaSql =
                    "create schema if not exists " + IdentifierUtil.quoteIdentifier(schemaName);
            String createSql =
                    "create table "
                            + TableName.quoteValueOf(schemaName, tableName).getFullName()
                            + "(id int not null, "
                            + IdentifierUtil.quoteIdentifier(colName1)
                            + " int, "
                            + colName2
                            + " int, "
                            + colName3
                            + " int, primary key(id));\n ";
            String createIndexSql1 =
                    "create global index "
                            + IdentifierUtil.quoteIdentifier(indexName1)
                            + " on "
                            + TableName.quoteValueOf(schemaName, tableName).getFullName()
                            + "("
                            + colName2
                            + ", "
                            + colName3
                            + ") include ("
                            + IdentifierUtil.quoteIdentifier(colName1)
                            + ");\n";
            String createIndexSql2 =
                    "create global index "
                            + IdentifierUtil.quoteIdentifier(indexName2)
                            + " on "
                            + TableName.quoteValueOf(schemaName, tableName).getFullName()
                            + "("
                            + IdentifierUtil.quoteIdentifier(colName1)
                            + ", col2) include (col3) with (distribution_key=col2, clustering_key=col2);\n";
            execute(
                    conn,
                    new String[] {
                        dropSql,
                        "begin;",
                        createSchemaSql,
                        createSql,
                        createIndexSql1,
                        createIndexSql2,
                        "commit;"
                    });

            try {
                TableSchema schema =
                        client.getTableSchema(TableName.quoteValueOf(schemaName, tableName), true);
                GlobalIndex[] globalIndexs = schema.getGlobalIndexs();
                Assert.assertEquals(globalIndexs.length, 2);
                Assert.assertEquals(
                        globalIndexs[0].getIndexName(),
                        TableName.quoteValueOf(schemaName, indexName1));
                Assert.assertEquals(
                        globalIndexs[1].getIndexName(),
                        TableName.quoteValueOf(schemaName, indexName2));
                Assert.assertEquals(
                        globalIndexs[0].getIndexKeys(), new String[] {colName2, colName3});
                Assert.assertEquals(
                        globalIndexs[1].getIndexKeys(), new String[] {colName1, colName2});
                Assert.assertEquals(
                        schema.toString(),
                        String.format(
                                "TableSchema{\n"
                                        + "tableId='%s', \n"
                                        + "schemaVersion='%S', \n"
                                        + "tableName=%s, \n"
                                        + "distributionKeys=[id], \n"
                                        + "clusteringKey=null, \n"
                                        + "segmentKey=null, \n"
                                        + "partitionInfo='null', \n"
                                        + "orientation='column', \n"
                                        + "binlogLevel=NONE, \n"
                                        + "columns=[\n"
                                        + "Column{name='id', typeName='int4', not null, primary key}, \n"
                                        + "Column{name='"
                                        + colName1
                                        + "', typeName='int4'}, \n"
                                        + "Column{name='col2', typeName='int4'}, \n"
                                        + "Column{name='col3', typeName='int4'}], \n"
                                        + "globalIndexInfo=[{"
                                        + TableName.quoteValueOf(schemaName, indexName1)
                                                .getFullName()
                                        + ":[col2, col3]},\n"
                                        + "{"
                                        + TableName.quoteValueOf(schemaName, indexName2)
                                                .getFullName()
                                        + ":["
                                        + colName1
                                        + ", col2]}\n"
                                        + "]}",
                                schema.getTableId(),
                                schema.getSchemaVersion(),
                                TableName.quoteValueOf(schemaName, tableName).getFullName()));
            } finally {
                execute(conn, new String[] {dropSql});
            }
        }
    }
}
