package com.alibaba.ververica.connectors.hologres.jdbc.serde;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.ververica.connectors.hologres.jdbc.config.HologresParams;
import org.postgresql.model.TableSchema;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * Holo RowConverter.
 */
public class HologresRowConverter implements Serializable {
	private final LogicalType[] fieldTypes;
	private final String[] fieldNames;
	private HologresParams param;
	private HologresValueConverter[] valueConverters;
	private RowData.FieldGetter[] fieldGetters;
	private int[] columnMapping;

	public HologresRowConverter(String[] fieldNames,
								LogicalType[] fieldTypes,
								HologresParams param) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.param = param;
		this.valueConverters = new HologresValueConverter[fieldNames.length];
		this.fieldGetters = new RowData.FieldGetter[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			this.fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
		}
	}

	public void convertToHologresRecord(Record record, RowData row) {
		if (null != row) {
			for (int i = 0; i < fieldNames.length; ++i) {
				Object obj = valueConverters[i].toHologresValue(fieldGetters[i].getFieldOrNull(row));
				record.setObject(columnMapping[i], obj);
			}
		}
	}

	public GenericRowData convertFromHologresRecord(Record record) throws SQLException {
		GenericRowData resultRow = new GenericRowData(fieldNames.length);
		for (int i = 0; i < fieldNames.length; i++) {
			resultRow.setField(i, valueConverters[i].fromHologresValue(record.getObject(fieldNames[i])));
		}
		return resultRow;
	}

	public void setHologresColumnSchema(TableSchema schema) {
		columnMapping = new int[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			boolean match = false;
			for (int j = 0; j < schema.getColumns().length; ++j) {
				if (schema.getColumns()[j].equals(fieldNames[i])) {
					match = true;
					columnMapping[i] = j;
					valueConverters[i] = HologresValueConverter.create(param, schema.getColumnTypes()[j], schema.getTypeNames()[j], fieldTypes[i]);
					if (!valueConverters[i].acceptFlinkType(fieldTypes[i])) {
						throw new IllegalArgumentException(String.format("Column: %s type does not match: flink row type: %s, hologres type: %s",
								fieldNames[i], fieldTypes[i], schema.getTypeNames()[j]));
					}
					break;
				}
			}
			if (!match) {
				throw new IllegalArgumentException(String.format("Column: %s does not found on hologres table schema",
						fieldNames[i]));
			}
		}
	}
}

