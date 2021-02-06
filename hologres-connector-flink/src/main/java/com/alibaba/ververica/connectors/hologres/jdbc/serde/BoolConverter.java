package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

/**
 * BoolValueConverter.
 */
public class BoolConverter extends HologresValueConverter {
	public BoolConverter(HologresParams param, int hologresType) {
		super(param, hologresType);
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.BOOLEAN);
	}
}

