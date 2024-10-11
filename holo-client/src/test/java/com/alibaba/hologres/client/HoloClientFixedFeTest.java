package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.ConnectionHolder;
import com.alibaba.hologres.client.impl.util.ConnectionUtil;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * HoloClient Fixed Fe Tester.
 *
 * @version 1.0
 * @since <pre>09月 05, 2022</pre>
 */
public class HoloClientFixedFeTest extends HoloClientTestBase {
	/**
	 * connection on the old version holo(before 1.3).
	 */
	@Test
	public void testFixedFeConn001() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) > 0) {
			return;
		}
		// 推荐使用options=type=fixed
		String url1 = properties.getProperty("url") + "?options=type=fixed";
		try (Connection conn = DriverManager.getConnection(url1, properties)) {
		} catch (SQLException s) {
			if (!s.getMessage().contains("invalid command-line argument for server process: type=fixed")) {
				throw s;
			}
		}
		// 1.3 gateway也可以识别: options= -c a=b, 1.1会去set guc，然后抛出异常
		String url2 = properties.getProperty("url") + "?options= -c type=fixed";
		try (Connection conn = DriverManager.getConnection(url2, properties)) {
		} catch (SQLException s) {
			if (!s.getMessage().contains("unrecognized configuration parameter \"type\"")) {
				throw s;
			}
		}
	}

	/**
	 * Behavior of jdbc url options.
	 */
	@Test
	public void testJdbcUrlOptionsConn001() throws Exception {
		if (properties == null) {
			return;
		}
		String url = properties.getProperty("url");
		// 两个GUC都设置成功
		String url1 = url + (url.contains("?") ? "&" : "?")
				+ "options=-c statement_timeout=123456 -c idle_in_transaction_session_timeout=654321";
		try (Connection conn = DriverManager.getConnection(url1, properties)) {
			try (Statement stat = conn.createStatement()) {
				try (ResultSet rs = stat.executeQuery("show statement_timeout;")) {
					if (rs.next()) {
						Assert.assertEquals("123456ms", rs.getString(1));
					}
				}
				try (ResultSet rs = stat.executeQuery("show idle_in_transaction_session_timeout;")) {
					if (rs.next()) {
						Assert.assertEquals("654321ms", rs.getString(1));
					}
				}
			}
		}
		// "="必须与options连起来，否则不会识别到之后的参数
		String url2 = url + (url.contains("?") ? "&" : "?")
				+ "options = -c statement_timeout=123456 -c idle_in_transaction_session_timeout=654321";
		try (Connection conn = DriverManager.getConnection(url2, properties)) {
			try (Statement stat = conn.createStatement()) {
				try (ResultSet rs = stat.executeQuery("show statement_timeout;")) {
					if (rs.next()) {
						// 没set成功，不相等
						Assert.assertNotEquals("123456ms", rs.getString(1));
					}
				}
				try (ResultSet rs = stat.executeQuery("show idle_in_transaction_session_timeout;")) {
					if (rs.next()) {
						// 没set成功，不相等
						Assert.assertNotEquals("654321ms", rs.getString(1));
					}
				}
			}
		}
		// GUC需要通过-c设置才能生效，直接等于不行
		String url3 = url + (url.contains("?") ? "&" : "?") + "options=statement_timeout=123456";
		try (Connection conn = DriverManager.getConnection(url3, properties)) {
		} catch (SQLException s) {
			if (!s.getMessage().contains(
					"invalid command-line argument for server process: statement_timeout=123456")) {
				throw s;
			}
		}
		// 多个options= -c 只有最后一个生效
		String url4 = url + (url.contains("?") ? "&" : "?")
				+ "options= -c statement_timeout=123456&options= -c idle_in_transaction_session_timeout=654321";
		try (Connection conn = DriverManager.getConnection(url4, properties)) {
			try (Statement stat = conn.createStatement()) {
				try (ResultSet rs = stat.executeQuery("show statement_timeout;")) {
					if (rs.next()) {
						// 没set成功，不相等
						Assert.assertNotEquals("123456ms", rs.getString(1));
					}
				}
				try (ResultSet rs = stat.executeQuery("show idle_in_transaction_session_timeout;")) {
					if (rs.next()) {
						Assert.assertEquals("654321ms", rs.getString(1));
					}
				}
			}
		}
	}

	@Test
	public void testFixedFeUrl001() {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) < 0) {
			return;
		}
		HoloConfig config = buildConfig();
		ConnectionHolder connectionHolder = new ConnectionHolder(config, this, false, true);
		// 已有options=
		Assert.assertEquals(ConnectionUtil.generateFixedUrl("jdbc:postgresql://ip:port/db"),
			"jdbc:postgresql://ip:port/db?options=type=fixed%20");
		Assert.assertEquals(
			ConnectionUtil.generateFixedUrl("jdbc:postgresql://ip:port/db?options=-c statement_timeout=123456"),
			"jdbc:postgresql://ip:port/db?options=type=fixed%20-c statement_timeout=123456");
		Assert.assertEquals(ConnectionUtil.generateFixedUrl(
				"jdbc:postgresql://ip:port/db?options=-c statement_timeout=123456 -c "
					+ "idle_in_transaction_session_timeout=654321"),
			"jdbc:postgresql://ip:port/db?options=type=fixed%20-c statement_timeout=123456 -c "
				+ "idle_in_transaction_session_timeout=654321");
		// 包含&连接的其他参数
		Assert.assertEquals(
			ConnectionUtil.generateFixedUrl("jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable"),
			"jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable&options=type=fixed%20");
		Assert.assertEquals(ConnectionUtil.generateFixedUrl(
				"jdbc:postgresql://ip:port/db?currentSchema=test&options=-c statement_timeout=123456&sslmode=disable"),
			"jdbc:postgresql://ip:port/db?currentSchema=test&options=type=fixed%20-c "
				+ "statement_timeout=123456&sslmode=disable");
		Assert.assertEquals(ConnectionUtil.generateFixedUrl(
				"jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable&options=-c statement_timeout=123456 "
					+ "-c idle_in_transaction_session_timeout=654321"),
			"jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable&options=type=fixed%20-c "
				+ "statement_timeout=123456 -c idle_in_transaction_session_timeout=654321");
		// 多个options=(不应该这样写), 最后一个生效，因此我们fixed插入到最后一个options=之后
		Assert.assertEquals(ConnectionUtil.generateFixedUrl(
				"jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable&options=-c "
					+ "statement_timeout=123456&options= -c idle_in_transaction_session_timeout=654321"),
			"jdbc:postgresql://ip:port/db?currentSchema=test&sslmode=disable&options=-c "
				+ "statement_timeout=123456&options=type=fixed%20 -c idle_in_transaction_session_timeout=654321");
	}

	/**
	 * url已经包含"options= -c a=b"时，使用fixed fe mode的情况.
	 */
	@Test
	public void testFixedFeUrl002() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) < 0) {
			return;
		}

		String url = properties.getProperty("url");
		// url已经包含"options=", 保留原来的options
		String originOptions = "";
		String optionsKey = "options=";
		if (url.contains(optionsKey)) {
			int optionsStartIndex = url.indexOf(optionsKey);
			optionsStartIndex += optionsKey.length();
			int optionsEndIndex = url.indexOf("&", optionsStartIndex);
			if (optionsEndIndex == -1) {
				optionsEndIndex = url.length(); // 如果没有 & 则到字符串末尾
			}
			originOptions = url.substring(optionsStartIndex, optionsEndIndex);
		}
		/*
			期望url
			fe连接： jdbc:postgresql://ip:port/db?options=-c statement_timeout=123456 -c idle_in_transaction_session_timeout=654321
			fixed fe连接：jdbc:postgresql://ip:port/db?options=type=fixed%20-c statement_timeout=123456 -c idle_in_transaction_session_timeout=654321
		*/
		url += (url.contains("?") ? "&" : "?")
				+ "options=" + originOptions + " -c statement_timeout=123456 -c idle_in_transaction_session_timeout=654321";
		HoloConfig config = new HoloConfig();
		config.setJdbcUrl(url);
		config.setUsername(properties.getProperty("user"));
		config.setPassword(properties.getProperty("password"));
		config.setUseFixedFe(true);

		try (Connection conn = buildConnection()) {
			String tableName = "test_schema.\"holO_client_put_fixedfe_url\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null,\"nAme\" text,\"address\" text not null,primary key(id))";
			execute(conn, new String[]{createSchema, dropSql, createSql});

			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				for (int i = 0; i < 100; i++) {
					Put put = new Put(schema);
					put.setObject("id", i);
					put.setObject("nAme", "name0");
					put.setObject("address", "address0");
					client.put(put);
				}
				client.flush();
				try (Statement stat = conn.createStatement()) {
					try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
						if (rs.next()) {
							Assert.assertEquals(100, rs.getInt(1));
						}
					}
				}
				client.sql(clientConn -> {
					try (Statement stat = clientConn.createStatement()) {
						try (ResultSet rs = stat.executeQuery("show statement_timeout;")) {
							if (rs.next()) {
								Assert.assertEquals("123456ms", rs.getString(1));
							}
						}
						try (ResultSet rs = stat.executeQuery("show idle_in_transaction_session_timeout;")) {
							if (rs.next()) {
								Assert.assertEquals("654321ms", rs.getString(1));
							}
						}
					}
					return 0;
				});
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 测试1.1以及之前版本的实例打开fixed fe mode抛出的异常.
	 */
	@Test
	public void testNotSupportFixedFeExceptionInLowVersionHolo() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) > 0) {
			return;
		}
		HoloConfig config = buildConfig();
		config.setRetryCount(1);
		config.setUseFixedFe(true);

		try (Connection conn = buildConnection()) {
			String tableName = "test_schema.\"holO_client_put_not_support_fixedfe\"";
			String createSchema = "create schema if not exists test_schema";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName
					+ "(id int not null,\"nAme\" text,\"address\" text not null,primary key(id))";
			execute(conn, new String[]{createSchema, dropSql, createSql});

			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema = client.getTableSchema(tableName);
				Put put = new Put(schema);
				put.setObject("id", 0);
				put.setObject("nAme", "name0");
				put.setObject("address", "address0");
				client.put(put);
				client.flush();
			} catch (Exception e) {
				e.printStackTrace();
				Assert.assertTrue(e.getMessage().contains("FixedFe mode is only supported after hologres version 1"
						+ ".3"));
			} finally {
				execute(conn, new String[]{dropSql});
			}
		}
	}

	/**
	 * 不使用fixed fe mode时连接数的使用情况.
	 */
	@Test
	public void testConnectionNumberWhenUseFePut() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) < 0) {
			return;
		}
		String appName = "conn_number_when_use_fe_put";
		HoloConfig config = buildConfig();
		config.setConnectionSizeWhenUseFixedFe(2);
		config.setWriteThreadSize(15);
		config.setUseFixedFe(false);
		config.setAppName(appName);
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_conn_number_when_use_fe_put";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key"
					+ "(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			int count = 100;
			for (int i = 0; i < count; i++) {
				Put put = new Put(schema);
				put.setObject(0, i);
				put.setObject(1, "name1");
				put.setObject(2, "address2");
				client.put(put);
			}
			client.flush();

			Assert.assertEquals(15, getConnNumberOfAppName(conn, appName));

			Optional<Integer> ret = Optional.empty();
			try (Statement stat = conn.createStatement()) {
				try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
					rs.next();
					ret = Optional.of(rs.getInt(1));
				}
			}
			Assert.assertEquals(java.util.Optional.of(count), ret);
			execute(conn, new String[]{dropSql});
		}
	}

	/**
	 * 使用fixed fe mode时连接数的使用情况.
	 */
	@Test
	public void testConnectionNumberWhenUseFixedFePut() throws Exception {
		if (properties == null) {
			return;
		}
		if (holoVersion.compareTo(new HoloVersion(1, 3, 0)) < 0) {
			return;
		}
		String appName = "conn_number_when_use_fixed_fe_put";
		HoloConfig config = buildConfig();
		config.setConnectionSizeWhenUseFixedFe(1);
		config.setWriteThreadSize(15);
		config.setUseFixedFe(true);
		config.setAppName(appName);
		config.setWriteMode(WriteMode.INSERT_OR_UPDATE);
		try (Connection conn = buildConnection(); HoloClient client = new HoloClient(config)) {
			String tableName = "holo_client_conn_number_when_use_fixed_fe_put";
			String dropSql = "drop table if exists " + tableName;
			String createSql = "create table " + tableName + "(id int not null,name text,address text,primary key"
					+ "(id))";

			execute(conn, new String[]{dropSql, createSql});

			TableSchema schema = client.getTableSchema(tableName);

			int count = 100;
			for (int i = 0; i < count; i++) {
				Put put = new Put(schema);
				put.setObject(0, i);
				put.setObject(1, "name1");
				put.setObject(2, "address2");
				client.put(put);
			}
			client.flush();

			Assert.assertEquals(1, getConnNumberOfAppName(conn, appName));

			Optional<Integer> ret = Optional.empty();
			try (Statement stat = conn.createStatement()) {
				try (ResultSet rs = stat.executeQuery("select count(*) from " + tableName)) {
					rs.next();
					ret = Optional.of(rs.getInt(1));
				}
			}
			Assert.assertEquals(java.util.Optional.of(count), ret);
			execute(conn, new String[]{dropSql});
		}
	}

	private long getConnNumberOfAppName(Connection conn, String appName) throws SQLException {
		try (Statement stat = conn.createStatement()) {
			try (ResultSet rs = stat.executeQuery(
					"select application_name,backend_type,count(*) from pg_stat_activity where application_name like '%"
							+ appName + "%' group by application_name,backend_type;")) {
				if (rs.next()) {
					return rs.getLong(3);
				}
			}
		}
		return 0;
	}
}
