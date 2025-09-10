package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;

public class OnConflictActionTest extends HoloClientTestBase {

    @DataProvider(name = "useOnConflictAction")
    public Object[] createData() {
        Object[] ret = new Object[2];
        ret[0] = true;
        ret[1] = false;
        return ret;
    }

    /** INSERT_OR_REPLACE. Method: put(Put put). */
    @Test(dataProvider = "useOnConflictAction")
    public void testPutReplace(boolean useOnConflictAction) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        if (useOnConflictAction) {
            config.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
        } else {
            config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
        }
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_OnConflictActionTest_001";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "address");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(1, "name1");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name3");
            client.put(put);
            client.flush();

            Record r = client.get(new Get(schema, new Object[] {0})).get();
            Assert.assertEquals("name3", r.getObject(1));
            Assert.assertNull(r.getObject(2));

            r = client.get(new Get(schema, new Object[] {1})).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertNull(r.getObject(2));

            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT_OR_UPDATE. Method: put(Put put). */
    @Test(dataProvider = "useOnConflictAction")
    public void testPutUpdate(boolean useOnConflictAction) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        if (useOnConflictAction) {
            config.setOnConflictAction(OnConflictAction.INSERT_OR_UPDATE);
        } else {
            config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
        }
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_OnConflictActionTest_002";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "address");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(1, "name1");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name3");
            client.put(put);
            client.flush();

            Record r = client.get(new Get(schema, new Object[] {0})).get();
            Assert.assertEquals("name3", r.getObject(1));
            Assert.assertEquals("address", r.getObject(2));

            r = client.get(new Get(schema, new Object[] {1})).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertNull(r.getObject(2));

            execute(conn, new String[] {dropSql});
        }
    }

    /** INSERT_OR_IGNORE. Method: put(Put put). */
    @Test(dataProvider = "useOnConflictAction")
    public void testPutIgnore(boolean useOnConflictAction) throws Exception {
        if (properties == null) {
            return;
        }
        HoloConfig config = buildConfig();
        if (useOnConflictAction) {
            config.setOnConflictAction(OnConflictAction.INSERT_OR_IGNORE);
        } else {
            config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
        }
        try (Connection conn = buildConnection();
                HoloClient client = new HoloClient(config)) {
            String tableName = "holo_client_OnConflictActionTest_003";
            String dropSql = "drop table if exists " + tableName;
            String createSql =
                    "create table "
                            + tableName
                            + "(id int not null,name text,address text,primary key(id))";

            execute(conn, new String[] {dropSql, createSql});

            TableSchema schema = client.getTableSchema(tableName);

            Put put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name0");
            put.setObject(2, "address");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 1);
            put.setObject(1, "name1");
            client.put(put);
            client.flush();

            put = new Put(schema);
            put.setObject(0, 0);
            put.setObject(1, "name3");
            client.put(put);
            client.flush();

            Record r = client.get(new Get(schema, new Object[] {0})).get();
            Assert.assertEquals("name0", r.getObject(1));
            Assert.assertEquals("address", r.getObject(2));

            r = client.get(new Get(schema, new Object[] {1})).get();
            Assert.assertEquals("name1", r.getObject(1));
            Assert.assertNull(r.getObject(2));

            execute(conn, new String[] {dropSql});
        }
    }
}
