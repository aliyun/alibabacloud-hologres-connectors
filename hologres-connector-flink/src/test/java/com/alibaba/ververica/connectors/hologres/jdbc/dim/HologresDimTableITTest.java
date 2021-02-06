package com.alibaba.ververica.connectors.hologres.jdbc.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import com.alibaba.ververica.connectors.hologres.jdbc.HologresTestBase;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for Dim Table.
 */
public class HologresDimTableITTest {
	protected EnvironmentSettings streamSettings;

	protected StreamExecutionEnvironment env;
	protected StreamTableEnvironment tEnv;

	protected HologresTestBase testConfig = null;

	private static final Long[] j = {8589934592L, 8589934593L, 8589934594L};
	private static final Float[] l = {8.58967f, 96.4667f, 9345.16f};
	private static final Double[] m = {587897.4646746, 792343.646446, 76.46464};
	private static final String[] n = {"monday", "saturday", "sunday"};
	private static final Integer[] o = {464, 98661, 32489};
	private static final Boolean[] p = {true, true, false, true};
	private static final Object[] expected = new Object[]{
			Row.of("Hi", 1, "dim", false, 652482L, LocalDate.of(2020, 7, 8), "source_test",
					LocalDateTime.of(LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
					j, n, o, p, BigDecimal.valueOf(1357926541357912L, 2), BigDecimal.valueOf(811923, 2), true,
					LocalDateTime.of(LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 123456000))),
			Row.of("Hi2", 1, "dim", false, 652482L, LocalDate.of(2020, 7, 8), "source_test",
					LocalDateTime.of(LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 737000000)),
					j, n, o, p, BigDecimal.valueOf(1357926541357912L, 2), BigDecimal.valueOf(811923, 2), true,
					LocalDateTime.of(LocalDate.of(2020, 07, 10), LocalTime.of(16, 28, 7, 123456000))),
			Row.of("AllNull", 1024, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
	};

	@Before
	public void prepareODPSTable() throws Exception {
		EnvironmentSettings.Builder streamBuilder = EnvironmentSettings.newInstance().inStreamingMode();

		this.streamSettings = streamBuilder.useBlinkPlanner().build();

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(env, streamSettings);

		List<Row> data = new ArrayList<>();
		data.add(Row.of(1, "Hi"));
		data.add(Row.of(2, "Hello"));
		data.add(Row.of(5, "Hello word"));
		data.add(Row.of(6, "Narotu"));
		data.add(Row.of(7, "N/A"));
		data.add(Row.of(1, "Hi2"));
		data.add(Row.of(1024, "AllNull"));
		createScanTable(data);

		testConfig = new HologresTestBase();
	}

	private void createScanTable(List<Row> data) {
		String dataId = TestValuesTableFactory.registerData(data);
		tEnv.executeSql(
				"create table MyTable (\n" +
						"  name int,\n" +
						"  ip STRING,\n" +
						"  proctime as PROCTIME()\n" +
						") with (\n" +
						"  'connector'='values',\n" +
						"  'data-id'='" + dataId + "'\n" +
						")");
	}

	@Test
	public void testDimTable() {
		String dimTable = "dim";
		tEnv.executeSql(
				"create table " + dimTable + "(\n" +
						"b STRING not null,\n" +
						"a int not null,\n" +
						"c double not null,\n" +
						"d boolean,\n" +
						"e bigint,\n" +
						"f date,\n" +
						"g varchar,\n" +
						"h TIMESTAMP,\n" +
						"i float,\n" +
						"j array<bigint>,\n" +
						"l array<float>,\n" +
						"m array<double>,\n" +
						"n array<STRING>,\n" +
						"o array<int>,\n" +
						"p array<boolean>,\n" +
						"q Decimal(38, 2),\n" +
						"r Decimal(6,2),\n" +
						"t timestamp,\n" +
						"s boolean\n" +
						") with (" +
						"'connector'='hologres-jdbc',\n" +
						"'endpoint'='" + testConfig.endpoint + "',\n" +
						"'dbname'='" + testConfig.database + "',\n" +
						"'tablename'='" + testConfig.dimTable + "',\n" +
						"'username'='" + testConfig.username + "',\n" +
						"'password'='" + testConfig.password + "'\n" +
						")");

		TableResult result = tEnv.executeSql("SELECT T.ip, a, b, d, e, f, g, h, j, n, o, p, q, r, s, t FROM MyTable AS T JOIN " +
				dimTable + " FOR SYSTEM_TIME AS OF T.proctime " +
				"AS H ON T.name = H.a");
		Object[] actual = Lists.newArrayList(result.collect()).toArray();
		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void testDimTableAsync() {
		String dimTable = "dim";
		tEnv.executeSql(
				"create table " + dimTable + "(\n" +
						"a int not null,\n" +
						"b STRING not null,\n" +
						"c double not null,\n" +
						"d boolean,\n" +
						"e bigint,\n" +
						"f date,\n" +
						"g varchar,\n" +
						"h TIMESTAMP,\n" +
						"i float,\n" +
						"j array<bigint> not null,\n" +
						"l array<float> not null,\n" +
						"m array<double>,\n" +
						"n array<STRING>,\n" +
						"o array<int>,\n" +
						"p array<boolean>,\n" +
						"q Decimal(38, 2),\n" +
						"r Decimal(6,2),\n" +
						"t timestamp,\n" +
						"s boolean\n" +
						") with (" +
						"'connector'='hologres-jdbc',\n" +
						"'async'='true',\n" +
						"'endpoint'='" + testConfig.endpoint + "',\n" +
						"'dbname'='" + testConfig.database + "',\n" +
						"'tablename'='" + testConfig.dimTable + "',\n" +
						"'username'='" + testConfig.username + "',\n" +
						"'password'='" + testConfig.password + "'\n" +
						")");

		TableResult result = tEnv.executeSql("SELECT T.ip, a, b, d, e, f, g, h, j, n, o, p, q, r, s, t FROM MyTable AS T JOIN " +
				dimTable + " FOR SYSTEM_TIME AS OF T.proctime " +
				"AS H ON T.name = H.a");
		Object[] actual = Lists.newArrayList(result.collect()).toArray();
		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void testDimTableWithPrimaryKey() {
		String dimTable = "dim";
		tEnv.executeSql(
				"create table " + dimTable + "(\n" +
						"b STRING not null,\n" +
						"a int not null,\n" +
						"c double not null,\n" +
						"d boolean,\n" +
						"e bigint,\n" +
						"f date,\n" +
						"g varchar,\n" +
						"h TIMESTAMP,\n" +
						"i float,\n" +
						"j array<bigint>,\n" +
						"l array<float>,\n" +
						"m array<double>,\n" +
						"n array<STRING>,\n" +
						"o array<int>,\n" +
						"p array<boolean>,\n" +
						"q Decimal(38, 2),\n" +
						"r Decimal(6,2),\n" +
						"t timestamp,\n" +
						"s boolean, \n" +
						"primary key(a) not enforced\n" +
						") with (" +
						"'connector'='hologres-jdbc',\n" +
						"'endpoint'='" + testConfig.endpoint + "',\n" +
						"'dbname'='" + testConfig.database + "',\n" +
						"'tablename'='public." + testConfig.dimTable + "',\n" +
						"'username'='" + testConfig.username + "',\n" +
						"'password'='" + testConfig.password + "'\n" +
						")");

		TableResult result = tEnv.executeSql("SELECT T.ip, a, b, d, e, f, g, h, j, n, o, p, q, r, s, t FROM MyTable AS T JOIN " +
				dimTable + " FOR SYSTEM_TIME AS OF T.proctime " +
				"AS H ON T.name = H.a");
		Object[] actual = Lists.newArrayList(result.collect()).toArray();
		Assert.assertArrayEquals(expected, actual);
	}
}
