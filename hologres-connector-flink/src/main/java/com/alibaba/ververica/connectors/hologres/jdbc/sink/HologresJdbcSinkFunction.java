package com.alibaba.ververica.connectors.hologres.jdbc.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import com.alibaba.ververica.connectors.common.sink.OutputFormatSinkFunction;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

/**
 * HologresJdbcSinkFunction.
 */
public class HologresJdbcSinkFunction extends OutputFormatSinkFunction<RowData> {
	public HologresJdbcSinkFunction(
			TableSchema schema,
			HologresParams params
	) {
		super(new HologresJdbcOutputFormat(schema, params));
	}
}
