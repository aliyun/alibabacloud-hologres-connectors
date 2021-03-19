/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.connectors.hologres.config;

import org.apache.flink.configuration.ReadableConfig;

import com.alibaba.hologres.client.model.WriteMode;
import com.alibaba.ververica.connectors.common.source.resolver.DirtyDataStrategy;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJdbcConfigs;
import com.alibaba.ververica.connectors.hologres.utils.JDBCUtils;

import java.io.Serializable;

/** A utility class of keeping the Hologres connection params. */
public class HologresConnectionParam implements Serializable {
    private static final long serialVersionUID = 1371382980680745051L;

    private final JDBCOptions options;

    // sink configuration
    private final int splitDataSize;
    private final boolean ignoreDelete;
    private final boolean createMissingPartTable;
    private final boolean ignoreNullWhenUpdate;

    // dirty data strategy
    private DirtyDataStrategy dirtyDataStrategy = DirtyDataStrategy.EXCEPTION;

    // JDBC
    private final int connectionPoolSize;
    private final int jdbcWriteBatchSize;
    private final int jdbcWriteFlushInterval;
    private final boolean insertIfNotExists;
    private final WriteMode writeMode;

    public HologresConnectionParam(ReadableConfig properties) {
        this.options = JDBCUtils.getJDBCOptions(properties);
        this.writeMode = getJDBCWriteMode(properties);

        this.createMissingPartTable =
                properties.get(HologresConfigs.CREATE_MISSING_PARTITION_TABLE);
        this.ignoreNullWhenUpdate = properties.get(HologresConfigs.IGNORE_NULL_WHEN_UPDATE);

        this.splitDataSize = properties.get(HologresConfigs.OPTIONAL_SPLIT_DATA_SIZE);
        this.ignoreDelete = properties.get(HologresConfigs.OPTIONAL_SINK_IGNORE_DELETE);

        String actionOnInsertError = properties.get(HologresConfigs.ACTION_ON_INSERT_ERROR);
        if (actionOnInsertError.equalsIgnoreCase("SKIP")) {
            this.dirtyDataStrategy = DirtyDataStrategy.SKIP;
        } else if (actionOnInsertError.equalsIgnoreCase("SKIP_SILENT")) {
            this.dirtyDataStrategy = DirtyDataStrategy.SKIP_SILENT;
        }

        this.connectionPoolSize =
                properties.get(HologresJdbcConfigs.OPTIONAL_CLIENT_CONNECTION_POOL_SIZE);
        this.jdbcWriteBatchSize =
                properties.get(HologresJdbcConfigs.OPTIONAL_JDBC_WRITE_BATCH_SIZE);
        this.jdbcWriteFlushInterval =
                properties.get(HologresJdbcConfigs.OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL);
        this.insertIfNotExists = properties.get(HologresJdbcConfigs.INSERT_IF_NOT_EXISTS);
    }

    public static WriteMode getJDBCWriteMode(ReadableConfig tableProperties) {
        WriteMode writeMode = WriteMode.INSERT_OR_IGNORE;
        if (tableProperties.get(HologresConfigs.INSERT_OR_UPDATE)) {
            writeMode = WriteMode.INSERT_OR_UPDATE;
        }
        if (tableProperties.getOptional(HologresConfigs.MUTATE_TYPE).isPresent()) {
            String mutateType = tableProperties.get(HologresConfigs.MUTATE_TYPE).toLowerCase();
            switch (mutateType) {
                case "insertorignore":
                    writeMode = WriteMode.INSERT_OR_IGNORE;
                    break;
                case "insertorreplace":
                    writeMode = WriteMode.INSERT_OR_REPLACE;
                    break;
                case "insertorupdate":
                    writeMode = WriteMode.INSERT_OR_UPDATE;
                    break;
                default:
                    throw new RuntimeException("Could not recognize mutate type " + mutateType);
            }
        }
        return writeMode;
    }

    public JDBCOptions getJdbcOptions() {
        return this.options;
    }

    public int getConnectionPoolSize() {
        return this.connectionPoolSize;
    }

    public String getDatabase() {
        return options.getDatabase();
    }

    public String getTable() {
        return options.getTable();
    }

    public String getEndpoint() {
        return options.getEndpoint();
    }

    public String getUsername() {
        return options.getUsername();
    }

    public String getPassword() {
        return options.getPassword();
    }

    public boolean isCreateMissingPartTable() {
        return createMissingPartTable;
    }

    public boolean isIgnoreNullWhenUpdate() {
        return ignoreNullWhenUpdate;
    }

    public int getSplitDataSize() {
        return splitDataSize;
    }

    public boolean isIgnoreDelete() {
        return ignoreDelete;
    }

    public DirtyDataStrategy getDirtyDataStrategy() {
        return dirtyDataStrategy;
    }

    public int getJdbcWriteBatchSize() {
        return this.jdbcWriteBatchSize;
    }

    public int getJdbcWriteFlushInterval() {
        return this.jdbcWriteFlushInterval;
    }

    public boolean isInsertIfNotExists() {
        return this.insertIfNotExists;
    }

    public WriteMode getJDBCWriteMode() {
        return this.writeMode;
    }

    @Override
    public String toString() {
        return "HologresConnectionParam{" + "options=" + options + '}';
    }
}
