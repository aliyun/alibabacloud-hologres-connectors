package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

/**
 * ShortConverter.
 */
public class ShortConverter extends HologresValueConverter {
	public ShortConverter(HologresParams param, int hologresType) {
		super(param, hologresType);
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.SMALLINT);
	}
}
