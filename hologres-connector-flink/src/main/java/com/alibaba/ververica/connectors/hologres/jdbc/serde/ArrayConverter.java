package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import org.postgresql.jdbc.PgArray;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ArrayConverter.
 */
public class ArrayConverter extends HologresValueConverter {
	private String delimiter;
	private HologresValueConverter converter;
	private LogicalType flinkType;
	private ArrayData.ElementGetter elementGetter;

	public ArrayConverter(HologresParams param, int hologresType, HologresValueConverter subConverter) {
		super(param, hologresType);
		this.delimiter = param.getDelimiter();
		this.converter = subConverter;
	}

	@Override
	public Object toHologresValue(Object value) {
		if (value == null) {
			return null;
		}
		List<Object> arrayCellList = new ArrayList<>();
		if (value instanceof StringData) {
			String stringValue = String.valueOf(value.toString());
			if (stringValue.length() > 0) {
				String[] arrayObject = stringValue.split(delimiter);
				for (String cell : arrayObject) {
					arrayCellList.add(cell);
				}
			}
		} else {
			ArrayData arrayData = (ArrayData) value;
			for (int i = 0; i < arrayData.size(); ++i) {
				arrayCellList.add(converter.toHologresValue(elementGetter.getElementOrNull(arrayData, i)));
			}
		}
		return arrayCellList;
	}

	@Override
	public Object fromHologresValue(Object value) throws SQLException {
		if (value == null) {
			return null;
		}
		Preconditions.checkArgument(value instanceof PgArray, "value should be instance of PgArray");
		PgArray pgArray = (PgArray) value;
		LogicalTypeRoot childType = flinkType.getChildren().get(0).getTypeRoot();
		switch (childType) {
			case BOOLEAN:
				return new GenericArrayData((Boolean[]) pgArray.getArray());
			case VARCHAR:
				String[] values = (String[]) pgArray.getArray();
				StringData[] arrStringObject = new StringData[values.length];
				for (int i = 0; i < values.length; ++i){
					arrStringObject[i] = StringData.fromString(values[i]);
				}
				return new GenericArrayData(arrStringObject);
			case INTEGER:
				return new GenericArrayData((Integer[]) pgArray.getArray());
			case FLOAT:
				return new GenericArrayData((Float[]) pgArray.getArray());
			case DOUBLE:
				return new GenericArrayData((Double[]) pgArray.getArray());
			case BIGINT:
				return new GenericArrayData((Long[]) pgArray.getArray());
			default:
				throw new UnsupportedOperationException("Currently reading array data of type " + childType + " is not supported");
		}
	}

	@Override
	public boolean acceptFlinkType(LogicalType flinkType) {
		this.flinkType = flinkType;
		if (flinkType.getChildren() != null && flinkType.getChildren().size() > 0) {
			this.elementGetter = ArrayData.createElementGetter(flinkType.getChildren().get(0));
		}
		// Hologres currently does not support Array of array.
		return flinkType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR) ||
				(flinkType.getTypeRoot().equals(LogicalTypeRoot.ARRAY) &&
						(flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.BOOLEAN)
								|| flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.VARCHAR)
								|| flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.INTEGER)
								|| flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.FLOAT)
								|| flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.DOUBLE)
								|| flinkType.getChildren().get(0).getTypeRoot().equals(LogicalTypeRoot.BIGINT)
						));
	}
}
