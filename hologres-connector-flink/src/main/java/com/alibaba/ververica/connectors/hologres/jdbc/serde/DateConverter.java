package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

import java.sql.Date;

/**
 * DateConverter.
 */
public class DateConverter extends HologresValueConverter {
	public DateConverter(HologresParams param, int hologresType) {
		super(param, hologresType);
	}

	@Override
	public Object toHologresValue(Object value) {
		if (value == null) {
			return null;
		}
		return new Date((int) value);
	}

	@Override
	public Object fromHologresValue(Object obj) {
		if (obj == null) {
			return null;
		}
		return (int) (((Date) obj).getTime() / 86400000) + 1;
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.DATE);
	}
}
