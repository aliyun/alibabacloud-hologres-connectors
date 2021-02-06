/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.jdbc;

import org.junit.Before;

import java.io.IOException;

import static org.junit.Assume.assumeNotNull;

/**
 * Test configs.
 */
public class HologresTestBase {
	private static final String ACCESS_ID = "HOLO_ACCESS_ID";
	private static final String ACCESS_KEY = "HOLO_ACCESS_KEY";
	private static final String ENDPOINT = "HOLO_ENDPOINT";
	private static final String TEST_DB = "HOLO_TEST_DB";
	private static final String TEST_DIM_TABLE = "HOLO_TEST_DIM_TABLE";
	private static final String TEST_SINK_TABLE = "HOLO_TEST_SINK_TABLE";
	private static final String TEST_EMPTY_TABLE = "HOLO_TEST_EMPTY_TABLE";

	public String endpoint;
	public String database;
	public String username;
	public String password;
	public String dimTable;
	public String sinkTable;
	public String emptyValueTable;

	@Before
	public void init() {
		assumeNotNull(System.getenv(ACCESS_ID));
		assumeNotNull(System.getenv(ACCESS_KEY));
		assumeNotNull(System.getenv(ENDPOINT));
	}

	public HologresTestBase() throws IOException {
		endpoint = System.getenv(ENDPOINT);
		database = System.getenv(TEST_DB);
		username = System.getenv(ACCESS_ID);
		password = System.getenv(ACCESS_KEY);
		dimTable = System.getenv(TEST_DIM_TABLE);
		sinkTable = System.getenv(TEST_SINK_TABLE);
		emptyValueTable = System.getenv(TEST_EMPTY_TABLE);
	}
}
