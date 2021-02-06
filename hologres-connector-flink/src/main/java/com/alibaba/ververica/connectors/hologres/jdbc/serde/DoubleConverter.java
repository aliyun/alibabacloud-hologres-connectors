package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

/**
 * DoubleConverter.
 */
public class DoubleConverter extends HologresValueConverter {
	public DoubleConverter(HologresParams param, int hologresType) {
		super(param, hologresType);
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.DOUBLE);
	}
}
