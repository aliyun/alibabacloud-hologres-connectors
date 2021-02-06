package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

import java.math.BigDecimal;

/**
 * DecimalConverter.
 */
public class DecimalConverter extends HologresValueConverter {
	private final LogicalType logicalType;

	public DecimalConverter(HologresParams param, int hologresType, LogicalType logicalType) {
		super(param, hologresType);
		this.logicalType = logicalType;
	}

	@Override
	public Object toHologresValue(Object value) {
		if (value == null) {
			return null;
		}
		DecimalData decimalData = (DecimalData) value;
		return decimalData.toBigDecimal();
	}

	@Override
	public Object fromHologresValue(Object record) {
		if (record == null) {
			return null;
		}
		BigDecimal bd = (BigDecimal) record;
		return DecimalData.fromBigDecimal(bd, ((DecimalType) logicalType).getPrecision(), bd.scale());
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.DECIMAL);
	}
}
