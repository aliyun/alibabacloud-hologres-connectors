package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

/**
 * StringConverter.
 */
public class StringConverter extends HologresValueConverter {
	public StringConverter(HologresParams param, int hologresType) {
		super(param, hologresType);
	}

	@Override
	public Object toHologresValue(Object value) {
		if (value == null) {
			return null;
		}
		StringData stringData = (StringData) value;
		return stringData.toString();
	}

	@Override
	public Object fromHologresValue(Object record) {
		if (record == null) {
			return null;
		}
		String str = (String) record;
		return StringData.fromString(str);
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR);
	}
}
