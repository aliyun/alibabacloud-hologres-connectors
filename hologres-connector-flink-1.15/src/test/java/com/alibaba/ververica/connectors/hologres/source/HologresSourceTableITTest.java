/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.source;

import org.apache.flink.table.api.TableResult;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.ververica.connectors.hologres.HologresTestBase;
import com.alibaba.ververica.connectors.hologres.HologresTestUtils;
import org.apache.commons.compress.utils.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;

/** HologresSourceTableITTest. */
public class HologresSourceTableITTest extends HologresTestBase {
    String prepareCreateTableSql =
            "BEGIN;\n"
                    + "CREATE TABLE IF NOT EXISTS TABLE_NAME (\n"
                    + "    a integer NOT NULL,\n"
                    + "    b text,\n"
                    + "    c double precision,\n"
                    + "    d boolean,\n"
                    + "    e bigint,\n"
                    + "    f date,\n"
                    + "    g character varying,\n"
                    + "    h timestamp with time zone,\n"
                    + "    i real,\n"
                    + "    j bigint[],\n"
                    + "    l real[],\n"
                    + "    m double precision[],\n"
                    + "    n text[],\n"
                    + "    o integer[],\n"
                    + "    p boolean[],\n"
                    + "    r numeric(6, 2),\n"
                    + "    s timestamp without time zone,\n"
                    + "    t timestamp with time zone,\n"
                    + "    u json,\n"
                    + "    v jsonb,\n"
                    + "    PRIMARY KEY (a)\n"
                    + ");\n"
                    + "CALL set_table_property ('TABLE_NAME', 'orientation', 'row');\n"
                    + "END;";

    private static final Object[][] prepareInsertTableValues =
            new Object[][] {
                new Object[] {
                    1,
                    "dim",
                    20.2007,
                    false,
                    652482,
                    new java.sql.Date(120, 6, 8),
                    "source_test",
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    8.58965,
                    new long[] {8589934592L, 8589934593L, 8589934594L},
                    new float[] {8.58967018F, 96.4666977F, 9345.16016F},
                    new double[] {587897.464674600051, 792343.64644599997, 76.4646400000000028},
                    new String[] {"monday", "saturday", "sunday"},
                    new int[] {464, 98661, 32489},
                    new boolean[] {true, true, false, true},
                    new BigDecimal("8119.23"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    Timestamp.valueOf("2020-07-10 16:28:07.737"),
                    "{\"a\":\"bbbb\", \"c\":\"dddd\"}",
                    "{\"a\": \"bbbb\", \"c\": \"dddd\"}"
                },
            };

    private String sourceTableWithSchema;

    public HologresSourceTableITTest() throws IOException {}

    @Before
    public void setUp() throws Exception {
        this.sourceTable = "test_source_table_" + randomSuffix;
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", this.sourceTable), false);
        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(client, this.sourceTable, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }

        executeSql("create schema if not exists test", false);
        this.sourceTableWithSchema = "test." + sourceTable;
        executeSql("create schema if not exists test", true);
        executeSql(prepareCreateTableSql.replace("TABLE_NAME", this.sourceTableWithSchema), false);
        try (HoloClient client = getHoloClient()) {
            HologresTestUtils.insertValues(
                    client, this.sourceTableWithSchema, prepareInsertTableValues);
        } catch (HoloClientException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        executeSql("drop table if exists " + this.sourceTable, true);
        executeSql("drop table if exists " + this.sourceTableWithSchema, true);
    }

    @Test
    public void testSourceTable() {
        testSourceTable(sourceTable);
    }

    @Test
    public void testSourceTableWithSchema() {
        testSourceTable(sourceTableWithSchema);
    }

    private void testSourceTable(String tableName) {
        tEnv.executeSql(
                "create table source"
                        + "(\n"
                        + "b STRING not null,\n"
                        + "a int not null,\n"
                        + "c double not null,\n"
                        + "d boolean,\n"
                        + "e bigint,\n"
                        + "f date,\n"
                        + "g varchar,\n"
                        + "h TIMESTAMP,\n"
                        + "i float,\n"
                        + "j array<bigint>,\n"
                        + "l array<float>,\n"
                        + "m array<double>,\n"
                        + "n array<STRING>,\n"
                        + "o array<int>,\n"
                        + "p array<boolean>,\n"
                        + "r Decimal(6,2),\n"
                        + "u varchar,\n" // json
                        + "v varchar\n" // jsonb
                        + ") with ("
                        + "'connector'='hologres',\n"
                        // + "'serverless-computing.enabled'='true',\n"
                        + "'endpoint'='"
                        + endpoint
                        + "',\n"
                        + "'dbname'='"
                        + database
                        + "',\n"
                        + "'tablename'='"
                        + tableName
                        + "',\n"
                        + "'username'='"
                        + username
                        + "',\n"
                        + "'password'='"
                        + password
                        + "'\n"
                        + ")");

        String expected =
                "[+I[dim, 1, 20.2007, false, 652482, 2020-07-08, source_test, 2020-07-10T16:28:07.737, 8.58965, [8589934592, 8589934593, 8589934594], [8.58967, 96.4667, 9345.16], [587897.4646746, 792343.646446, 76.46464], [monday, saturday, sunday], [464, 98661, 32489], [true, true, false, true], 8119.23, {\"a\":\"bbbb\", \"c\":\"dddd\"}, {\"a\": \"bbbb\", \"c\": \"dddd\"}]]";
        TableResult tableResult = tEnv.executeSql("select * from source");
        Object[] actual = Lists.newArrayList(tableResult.collect()).toArray();
        Assert.assertEquals(expected, Arrays.toString(actual));
    }
}
