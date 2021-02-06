package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.ververica.connectors.hologres.jdbc.catalog.HologresCatalog;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Types;

/**
 * HologresValueConverter.
 */
public abstract class HologresValueConverter implements Serializable {
	protected HologresParams param;
	protected int hologresType;

	public HologresValueConverter(HologresParams param, int hologresType) {
		this.param = param;
		this.hologresType = hologresType;
	}

	public static HologresValueConverter create(HologresParams param, int hologresType, String typeName, LogicalType logicalType) {
		if (hologresType == Types.ARRAY) {
			int type = -1;
			switch (typeName) {
				case "_bool":
					type = Types.BIT;
					break;
				case "_int2":
					type = Types.SMALLINT;
					break;
				case "_int4":
					type = Types.INTEGER;
					break;
				case "_int8":
					type = Types.BIGINT;
					break;
				case "_float8":
					type = Types.DOUBLE;
					break;
				case "_float4":
					type = Types.FLOAT;
					break;
				case "_varcahr":
				case "_text":
					type = Types.VARCHAR;
					break;
				case "_numeric":
					type = Types.NUMERIC;
					break;
				case "_timestamptz":
				case "_timestamp":
					type = Types.TIMESTAMP;
					break;
				case "_date":
					type = Types.DATE;
					break;
				default:
					throw new UnsupportedOperationException("Currently does not support hologres array type " + typeName);
			}
			return new ArrayConverter(param, hologresType, createPrimitive(param, type, typeName, logicalType));
		} else {
			return createPrimitive(param, hologresType, typeName, logicalType);
		}
	}

	public static HologresValueConverter createPrimitive(HologresParams param, int hologresType, String typeName, LogicalType logicalType) {
		switch (hologresType) {
			case Types.BIT:
				return new BoolConverter(param, hologresType);
			case Types.CHAR:
				return new ByteConverter(param, hologresType);
			case Types.SMALLINT:
				return new ShortConverter(param, hologresType);
			case Types.INTEGER:
				return new IntegerConverter(param, hologresType);
			case Types.BIGINT:
				return new LongConverter(param, hologresType);
			case Types.FLOAT:
			case Types.REAL:
				return new FloatConverter(param, hologresType);
			case Types.DOUBLE:
				return new DoubleConverter(param, hologresType);
			case Types.VARCHAR:
				return new StringConverter(param, hologresType);
			case Types.DECIMAL:
			case Types.NUMERIC:
				return new DecimalConverter(param, hologresType, logicalType);
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
				return new TimestampConverter(param, hologresType, logicalType);
			case Types.DATE:
				return new DateConverter(param, hologresType);
			default:
				switch (typeName) {
					case HologresCatalog.PG_GEOMETRY:
					case HologresCatalog.PG_GEOGRAPHY:
					case HologresCatalog.PG_BOX2D:
					case HologresCatalog.PG_BOX3D:
						return new StringConverter(param, hologresType);
				}
				throw new UnsupportedOperationException("Currently does not support hologres type " + typeName);
		}
	}

	public Object toHologresValue(Object value) {
		return value;
	}

	public Object fromHologresValue(Object object) throws SQLException {
		return object;
	}

	public abstract boolean acceptFlinkType(LogicalType flinkType);
}
