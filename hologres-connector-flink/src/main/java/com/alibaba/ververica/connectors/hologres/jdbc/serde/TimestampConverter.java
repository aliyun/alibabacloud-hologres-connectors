package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

import java.sql.Timestamp;

/**
 * TimestampConverter.
 */
public class TimestampConverter extends HologresValueConverter {
	private LogicalType logicalType;
	private boolean withTimeZone;

	public TimestampConverter(HologresParams param, int hologresType, LogicalType logicalType) {
		super(param, hologresType);
		this.logicalType = logicalType;
		this.withTimeZone = logicalType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
	}

	@Override
	public Object toHologresValue(Object value) {
		if (null == value) {
			return null;
		}
		if (this.withTimeZone) {
			return new Timestamp(((TimestampData) value).getMillisecond());
		} else {
			return ((TimestampData) value).toTimestamp();
		}
	}

	@Override
	public Object fromHologresValue(Object record) {
		if (record == null) {
			return null;
		}
		return TimestampData.fromTimestamp((Timestamp) record);
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) ||
				flinkType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
	}
}
