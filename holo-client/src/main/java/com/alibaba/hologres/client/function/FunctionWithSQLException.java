/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.function;

import java.sql.SQLException;

/**
 * 带异常的function.
 *
 * @param <I> 输入
 * @param <O> 输出
 */
public interface FunctionWithSQLException<I, O> {
	O apply(I input) throws SQLException;
}
