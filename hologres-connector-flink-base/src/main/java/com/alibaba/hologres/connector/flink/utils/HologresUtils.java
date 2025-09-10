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

package com.alibaba.hologres.connector.flink.utils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.checkandput.CheckAndPutCondition;
import com.alibaba.hologres.client.model.checkandput.CheckCompareOp;
import com.alibaba.hologres.connector.flink.api.HologresTableSchema;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.connector.flink.config.WriteMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** HologresUtils. */
public class HologresUtils {

    public static String removeU0000(final String in) {
        if (in != null && in.contains("\u0000")) {
            return in.replaceAll("\u0000", "");
        } else {
            return in;
        }
    }

    public static CheckAndPutCondition getCheckAndPutCondition(ReadableConfig properties) {
        String columnName = properties.get(HologresConfigs.CHECK_AND_PUT_COLUMN);
        if (Objects.isNull(columnName)) {
            return null;
        }

        String operator = properties.get(HologresConfigs.CHECK_AND_PUT_OPERATOR);
        String nullAs = properties.get(HologresConfigs.CHECK_AND_PUT_NULL_AS);
        return new CheckAndPutCondition(columnName, CheckCompareOp.valueOf(operator), null, nullAs);
    }

    public static WriteMode chooseBestWriteMode(
            JDBCOptions jdbcOptions, boolean enableReshuffleByHolo) {
        TableSchema holoSchema = HologresTableSchema.get(jdbcOptions).get();
        HoloVersion holoVersion = JDBCUtils.getHoloVersion(jdbcOptions);

        // 2.2.25之后支持全字段时的bulk_load_onc_conflict, 3.1.0之后支持部分字段的bulk_load_on_conflict
        // 结果表指定了reshuffle参数的情况下, 才会选择bulk_load_on_conflict
        boolean supportBulkLoadOnConflict =
                enableReshuffleByHolo
                        && holoSchema.getPrimaryKeys().length > 0
                        && ((holoVersion.compareTo(new HoloVersion(2, 2, 25)) > 0
                                        && holoSchema.getColumnSchema().length
                                                == holoSchema.getColumnSchema().length)
                                || holoVersion.compareTo(new HoloVersion(3, 1, 0)) > 0);
        boolean supportBulkLoad =
                holoSchema.getPrimaryKeys().length == 0
                        && holoVersion.compareTo(new HoloVersion(2, 1, 0)) > 0;
        boolean supportStreamCopy = holoVersion.compareTo(new HoloVersion(1, 3, 24)) > 0;
        if (supportBulkLoadOnConflict) {
            return WriteMode.COPY_BULK_LOAD_ON_CONFLICT;
        } else if (supportBulkLoad) {
            return WriteMode.COPY_BULK_LOAD;
        } else if (supportStreamCopy) {
            return WriteMode.COPY_STREAM;
        } else {
            return WriteMode.INSERT;
        }
    }

    /** ExpressionExtractor: convert filter expression to holo sql. */
    public static class ExpressionExtractor extends ExpressionDefaultVisitor<Optional<String>> {

        // maps a supported function to its name
        private static final Map<FunctionDefinition, String> FUNC_TO_STR_ARGUMENT_2 =
                new HashMap<>();
        private static final Map<FunctionDefinition, String> FUNC_TO_STR_ARGUMENT_1 =
                new HashMap<>();

        static {
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.EQUALS, "=");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.NOT_EQUALS, "<>");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.GREATER_THAN, ">");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ">=");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.LESS_THAN, "<");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, "<=");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.AND, "and");
            FUNC_TO_STR_ARGUMENT_2.put(BuiltInFunctionDefinitions.OR, "or");

            FUNC_TO_STR_ARGUMENT_1.put(BuiltInFunctionDefinitions.IS_NOT_NULL, "(%s is not null)");
            FUNC_TO_STR_ARGUMENT_1.put(BuiltInFunctionDefinitions.IS_NULL, "(%s is null)");
            FUNC_TO_STR_ARGUMENT_1.put(BuiltInFunctionDefinitions.NOT, "(not(%s))");
        }

        @Override
        public Optional<String> visit(CallExpression call) {
            FunctionDefinition funcDef = call.getFunctionDefinition();
            if (FUNC_TO_STR_ARGUMENT_2.containsKey(funcDef)
                    || FUNC_TO_STR_ARGUMENT_1.containsKey(funcDef)) {
                List<String> operands = new ArrayList<>();
                for (Expression child : call.getChildren()) {
                    Optional<String> operand = child.accept(this);
                    if (!operand.isPresent()) {
                        return Optional.empty();
                    }
                    operands.add(operand.get());
                }
                if (operands.size() == 1 && FUNC_TO_STR_ARGUMENT_1.containsKey(funcDef)) {
                    return Optional.of(
                            String.format(FUNC_TO_STR_ARGUMENT_1.get(funcDef), operands.get(0)));
                } else if (operands.size() == 2 && FUNC_TO_STR_ARGUMENT_2.containsKey(funcDef)) {
                    return Optional.of(
                            "("
                                    + String.join(
                                            " " + FUNC_TO_STR_ARGUMENT_2.get(funcDef) + " ",
                                            operands)
                                    + ")");
                } else {
                    return Optional.empty();
                }
            }
            return Optional.empty();
        }

        @Override
        public Optional<String> visit(ValueLiteralExpression valueLiteral) {
            DataType dataType = valueLiteral.getOutputDataType();
            Object value = valueLiteral.getValueAs(Object.class).orElse(null);
            if (value == null) {
                return Optional.of("null");
            }
            LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
            String res = value.toString();
            if (typeRoot == LogicalTypeRoot.CHAR
                    || typeRoot == LogicalTypeRoot.VARCHAR
                    || typeRoot == LogicalTypeRoot.BINARY
                    || typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    || typeRoot == LogicalTypeRoot.DATE
                    || typeRoot == LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE) {
                res = "'" + res.replace("'", "''") + "'";
            }
            return Optional.ofNullable(res);
        }

        @Override
        public Optional<String> visit(FieldReferenceExpression fieldReference) {
            return Optional.of("\"" + fieldReference.getName() + "\"");
        }

        @Override
        public Optional<String> visit(TypeLiteralExpression typeLiteral) {
            return Optional.ofNullable(typeLiteral.getOutputDataType().toString());
        }

        @Override
        public Optional<String> visit(Expression other) {
            // only support resolved expressions
            return Optional.empty();
        }

        @Override
        protected Optional<String> defaultMethod(Expression expression) {
            // no default method
            return Optional.empty();
        }
    }
}
