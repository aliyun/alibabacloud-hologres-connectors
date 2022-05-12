/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client;

import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * ByteSize计算测试.
 * 保证比较大的字段会计算，不保证完全精确
 */
public class HoloClientByteSizeTest extends HoloClientTestBase {

	/**
	 * test Normal Array.
	 * Method: put(Put put).
	 */
	@Test
	public void testNormalArray() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_all_types";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(" +
					"a int," +
					"b bigint," +
					"c smallint," +
					"d float4," +
					"e float8," +
					"f boolean," +
					"g text," +
					"h decimal(18,5)," +
					"i date," +
					"j timestamp," +
					"k timestamptz," +
					"l json," +
					"m jsonb," +
					"n roaringbitmap," +
					"o int4[]," +
					"p int8[]," +
					"q float4[]," +
					"r float8[]," +
					"s boolean[]," +
					"t text[]," +
					"primary key(a))";

			execute(conn, new String[]{dropSql, createSql});
			long byteSize = 0;
			TableSchema schema = client.getTableSchema(tableName);
			Put put = new Put(schema);
			put.setObject(0, 1);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(1, 2L);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(2, 3);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 2);
			put.setObject(3, 12.34);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(4, 123.456);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(5, true);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 1);
			put.setObject(6, "abcdefghijk");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 11);
			put.setObject(7, 1234.5678);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 24);
			put.setObject(8, "2021-10-19");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(9, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(10, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(11, "{\"a\":\"cccc\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(12, "{\"a\":\"ccccdddd\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 16);
			put.setObject(13, new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 22);
			put.setObject(14, new int[]{58, 48, 38});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 3);
			put.setObject(15, new long[]{58L, 48L});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 2);
			put.setObject(16, new float[]{1.1f, 2.2f});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 2);
			put.setObject(17, new double[]{1.1d, 2.2d, 3.3d});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 3);
			put.setObject(18, new boolean[]{true, false, true});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 3);
			put.setObject(19, new String[]{"aaaaa", "bbbbbb", "ccccccc"});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += (5 + 6 + 7));

			client.put(put);
			System.out.println(put.getRecord().getByteSize());
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize);

			client.flush();
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * test Object Array.
	 * flush 操作进行时Object[]类型的Array会被转成PGArray插入
	 * Method: put(Put put).
	 */
	@Test
	public void testObjectArray() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_all_types_1";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(" +
					"a int," +
					"b bigint," +
					"c smallint," +
					"d float4," +
					"e float8," +
					"f boolean," +
					"g text," +
					"h decimal(18,5)," +
					"i date," +
					"j timestamp," +
					"k timestamptz," +
					"l json," +
					"m jsonb," +
					"n roaringbitmap," +
					"o int4[]," +
					"p int8[]," +
					"q float4[]," +
					"r float8[]," +
					"s boolean[]," +
					"t text[]," +
					"primary key(a))";

			execute(conn, new String[]{dropSql, createSql});
			long byteSize = 0;
			TableSchema schema = client.getTableSchema(tableName);
			Put put = new Put(schema);
			put.setObject(0, 1);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(1, 2L);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(2, 3);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 2);
			put.setObject(3, 12.34);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(4, 123.456);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(5, true);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 1);
			put.setObject(6, "abcdefghijk");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 11);
			put.setObject(7, 1234.5678);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 24);
			put.setObject(8, "2021-10-19");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(9, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(10, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(11, "{\"a\":\"cccc\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(12, "{\"a\":\"ccccdddd\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 16);
			put.setObject(13, new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 22);
			put.setObject(14, new Object[]{58, 48, 38});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 3);
			put.setObject(15, new Object[]{58L, 48L});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 2);
			put.setObject(16, new Object[]{1.1f, 2.2f});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 2);
			put.setObject(17, new Object[]{1.1d, 2.2d, 3.3d});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 3);
			put.setObject(18, new Object[]{true, false, true});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 3);
			put.setObject(19, new Object[]{"aaaaa", "bbbbbb", "ccccccc"});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += (5 + 6 + 7));

			client.put(put);
			System.out.println(put.getRecord().getByteSize());
			// 估计大小
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize);

			client.flush();
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * test ArrayList Array.
	 * flush 操作进行时ArrayList类型的Array会被转成PGArray插入
	 * Method: put(Put put).
	 */
	@Test
	public void testArrayListArray() throws Exception {
		if (properties == null) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_all_types_1";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(" +
					"a int," +
					"b bigint," +
					"c smallint," +
					"d float4," +
					"e float8," +
					"f boolean," +
					"g text," +
					"h decimal(18,5)," +
					"i date," +
					"j timestamp," +
					"k timestamptz," +
					"l json," +
					"m jsonb," +
					"n roaringbitmap," +
					"o int4[]," +
					"p int8[]," +
					"q float4[]," +
					"r float8[]," +
					"s boolean[]," +
					"t text[]," +
					"primary key(a))";

			execute(conn, new String[]{dropSql, createSql});
			long byteSize = 0;
			TableSchema schema = client.getTableSchema(tableName);
			Put put = new Put(schema);
			put.setObject(0, 1);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(1, 2L);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(2, 3);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 2);
			put.setObject(3, 12.34);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(4, 123.456);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8);
			put.setObject(5, true);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 1);
			put.setObject(6, "abcdefghijk");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 11);
			put.setObject(7, 1234.5678);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 24);
			put.setObject(8, "2021-10-19");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4);
			put.setObject(9, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(10, "2021-10-19 08:08:08");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(11, "{\"a\":\"cccc\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 12);
			put.setObject(12, "{\"a\":\"ccccdddd\"}");
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 16);
			put.setObject(13, new byte[]{58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0, 5, 0});
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 22);
			List<Object> intArray = new ArrayList<>();
			intArray.add(58);
			intArray.add(48);
			intArray.add(38);
			put.setObject(14, intArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 3);
			List<Object> longArray = new ArrayList<>();
			longArray.add(58L);
			longArray.add(48L);
			put.setObject(15, longArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 2);
			List<Object> floatArray = new ArrayList<>();
			floatArray.add(1.1f);
			floatArray.add(2.2f);
			put.setObject(16, floatArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 4 * 2);
			List<Object> doubleArray = new ArrayList<>();
			doubleArray.add(1.1d);
			doubleArray.add(2.2d);
			doubleArray.add(3.3d);
			put.setObject(17, doubleArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 8 * 3);
			List<Object> boolArray = new ArrayList<>();
			boolArray.add(true);
			boolArray.add(false);
			boolArray.add(true);
			put.setObject(18, boolArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += 3);
			List<Object> textArray = new ArrayList<>();
			textArray.add("aaaaa");
			textArray.add("bbbbbb");
			textArray.add("ccccccc");
			put.setObject(19, textArray);
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize += (5 + 6 + 7));

			client.put(put);
			System.out.println(put.getRecord().getByteSize());
			// 估计大小
			Assert.assertEquals(put.getRecord().getByteSize(), byteSize);

			client.flush();
			execute(conn, new String[]{dropSql});
		}
	}
}
