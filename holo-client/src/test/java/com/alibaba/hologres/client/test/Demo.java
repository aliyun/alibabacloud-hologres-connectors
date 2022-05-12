package com.alibaba.hologres.client.test;

import com.alibaba.hologres.client.Get;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A demo class.
 * */
public class Demo {
	public static void main(String[] args) {

		String url = "";
		String username = "";
		String password = "";

		Properties properties = new Properties();
		properties.put("user", username);
		properties.put("password", password);
		properties.put("dynamicPartition", "true");
		//prepare table
		try (Connection conn = DriverManager.getConnection(url, properties)) {
			String[] preSqls = new String[]{
				"drop table if exists t0",
				"drop table if exists t1",
				"drop table if exists t3",
				"create table t0(id int not null,name0 text,address text,primary key(id))",
				"create table t1(id int not null,age int)",
				"create table t3(id int not null,region text not null,name text,primary key(id,region)) partition by list(region)"
			};

			for (String sql : preSqls) {
				try (Statement stat = conn.createStatement()) {
					stat.execute(sql);
				}
			}

			HoloConfig config = new HoloConfig();
			config.setJdbcUrl(url);
			config.setUsername(username);
			config.setPassword(password);
			config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
			config.setDynamicPartition(true);
			try (HoloClient client = new HoloClient(config)) {
				TableSchema schema0 = client.getTableSchema("t0");
				TableSchema schema1 = client.getTableSchema("t1");
				TableSchema schema3 = client.getTableSchema("t3");

				//写有主键表
				{
					List<Put> putList = new ArrayList<>();
					Put put = new Put(schema0);
					put.setObject(0, 1);
					put.setObject(1, "name0");
					put.setObject(2, "address0");
					putList.add(put);
					put = new Put(schema0);
					put.setObject(0, 2);
					put.setObject(1, "name2");
					put.setObject(2, "address0");
					putList.add(put);
					put = new Put(schema0);
					put.setObject(0, 1);
					put.setObject(1, "name3");
					put.setObject(2, "address3");
					putList.add(put);
					client.put(putList);
					client.flush();

					try {
						Record record = client.get(new Get(schema0, new Object[]{1})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}
					try {
						Record record = client.get(new Get(schema0, new Object[]{2})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}
					try {
						Record record = client.get(new Get(schema0, new Object[]{3})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}

				}

				//写无主键表
				{
					List<Put> putList = new ArrayList<>();
					Put put = new Put(schema1);
					put.setObject(0, 1);
					put.setObject(1, 2);
					putList.add(put);
					put = new Put(schema1);
					put.setObject(0, 1);
					put.setObject(1, 2);
					putList.add(put);
					put = new Put(schema1);
					put.setObject(0, 1);
					put.setObject(1, 2);
					putList.add(put);
					put = new Put(schema1);
					put.setObject(0, 1);
					put.setObject(1, 3);
					putList.add(put);
					client.put(putList);
					client.flush();

					try (Statement stat = conn.createStatement()) {
						System.out.println("select * from t1-----------------------------------");
						try (ResultSet rs = stat.executeQuery("select * from t1")) {
							int columnCount = rs.getMetaData().getColumnCount();
							StringBuilder sb = new StringBuilder();
							while (rs.next()) {
								for (int i = 0; i < columnCount; ++i) {
									sb.append(rs.getObject(i + 1)).append(",");
								}
								System.out.println(sb.toString());
								sb.setLength(0);
							}
						}

						System.out.println("--------------------------------------------------");
					}
				}
				//写分区表
				{
					List<Put> putList = new ArrayList<>();
					Put put = new Put(schema3);
					put.setObject(0, 1);
					put.setObject(1, "region0");
					put.setObject(2, "address0");
					putList.add(put);
					put = new Put(schema3);
					put.setObject(0, 2);
					put.setObject(1, "region1");
					put.setObject(2, "address0");
					putList.add(put);
					put = new Put(schema3);
					put.setObject(0, 1);
					put.setObject(1, "region0");
					put.setObject(2, "address3");
					putList.add(put);
					client.put(putList);
					client.flush();
					try {
						Record record = client.get(new Get(schema3, new Object[]{1, "region0"})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}
					try {
						Record record = client.get(new Get(schema3, new Object[]{2, "region1"})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}
					try {
						Record record = client.get(new Get(schema3, new Object[]{3, "region"})).get();
						System.out.println(record);
					} catch (ExecutionException e) {
						e.getCause().printStackTrace();
					}

				}

			} catch (InterruptedException | HoloClientException e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
	}
}
