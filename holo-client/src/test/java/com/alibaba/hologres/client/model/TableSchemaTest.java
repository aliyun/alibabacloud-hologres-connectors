package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloClientTestBase;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.binlog.BinlogLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;

/**
 * TableSchema Tester.
 */
public class TableSchemaTest extends HoloClientTestBase {

	/**
	 * 无引号，有大写, 无特殊字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testBinlogLevel() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setWriteThreadSize(10);
		config.setBinlogReadBatchSize(20);

		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName1 = "holo_client_open_binlog";
			String tableName2 = "holo_client_not_open_binlog";

			String dropSql1 = "drop table if exists " + tableName1;
			String dropSql2 = "drop table if exists " + tableName2;

			String createSql1 = "create table " + tableName1
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n "
					+ "call set_table_property('" + tableName1 + "', 'binlog.level', 'replica');\n";

			String createSql2 = "create table " + tableName2
					+ "(id int not null, amount decimal(12,2), t text, ts timestamptz, ba bytea, t_a text[],i_a int[], primary key(id));\n ";
			execute(conn, new String[]{dropSql1, dropSql2, "begin;", createSql1, "commit;", "begin;", createSql2, "commit;"});

			try {
				TableSchema schema1 = client.getTableSchema(tableName1, true);
				Assert.assertEquals(schema1.getBinlogLevel(), BinlogLevel.REPLICA);
				TableSchema schema2 = client.getTableSchema(tableName2, true);
				Assert.assertEquals(schema2.getBinlogLevel(), BinlogLevel.NONE);
			} finally {
				execute(conn, new String[]{dropSql1, dropSql2});
			}
		}
	}

}
