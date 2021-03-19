package com.alibaba.ververica.connectors.hologres.jdbc;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.org.postgresql.jdbc.PgArray;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.api.table.RowDataReader;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/** Transform Record to RowData. */
public class HologresJDBCRecordReader implements RowDataReader<Record> {
    private final Map<Integer, Integer> flinkColumnToHologresColumn;

    public HologresJDBCRecordReader(String[] flinkColumnNames, HologresTableSchema tableSchema) {
        this.flinkColumnToHologresColumn = new HashMap<>();
        for (int i = 0; i < flinkColumnNames.length; i++) {
            Integer hologresColumnIndex = tableSchema.get().getColumnIndex(flinkColumnNames[i]);
            if (hologresColumnIndex == null || hologresColumnIndex < 0) {
                throw new RuntimeException(
                        "Hologres table does not have column " + flinkColumnNames[i]);
            }
            flinkColumnToHologresColumn.put(i, hologresColumnIndex);
        }
    }

    @Override
    public void checkHologresTypeSupported(int hologresType, String typeName) {}

    @Override
    public Boolean readBoolean(Record record, int index) {
        return (Boolean) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public Byte readByte(Record record, int index) {
        return (Byte) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public Short readShort(Record record, int index) {
        return (Short) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public Integer readInt(Record record, int index) {
        return (Integer) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public Float readFloat(Record record, int index) {
        return (Float) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public Double readDouble(Record record, int index) {
        return (Double) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public StringData readString(Record record, int index) {
        return StringData.fromString(
                (String) record.getObject(flinkColumnToHologresColumn.get(index)));
    }

    @Override
    public Integer readDate(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        return (int)
                        (((Date) record.getObject(flinkColumnToHologresColumn.get(index))).getTime()
                                / 86400000)
                + 1;
    }

    @Override
    public Long readLong(Record record, int index) {
        return (Long) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public TimestampData readTimestamptz(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        return TimestampData.fromTimestamp(
                (Timestamp) record.getObject(flinkColumnToHologresColumn.get(index)));
    }

    @Override
    public TimestampData readTimestamp(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        return TimestampData.fromTimestamp(
                (Timestamp) record.getObject(flinkColumnToHologresColumn.get(index)));
    }

    @Override
    public Object readObject(Record record, int index) {
        return record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public byte[] readBinary(Record record, int index) {
        return (byte[]) record.getObject(flinkColumnToHologresColumn.get(index));
    }

    @Override
    public DecimalData readDecimal(
            Record record, int index, int decimalPrecision, int decimalScale) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        BigDecimal bd = (BigDecimal) record.getObject(flinkColumnToHologresColumn.get(index));
        return DecimalData.fromBigDecimal(bd, decimalPrecision, decimalScale);
    }

    @Override
    public GenericArrayData readIntArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            return new GenericArrayData(
                    (Integer[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public GenericArrayData readLongArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            return new GenericArrayData(
                    (Long[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public GenericArrayData readFloatArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            return new GenericArrayData(
                    (Float[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public GenericArrayData readDoubleArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            return new GenericArrayData(
                    (Double[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public GenericArrayData readBooleanArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            return new GenericArrayData(
                    (Boolean[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    public GenericArrayData readStringArray(Record record, int index) {
        if (record.getObject(flinkColumnToHologresColumn.get(index)) == null) {
            return null;
        }
        try {
            String[] values =
                    (String[])
                            ((PgArray) record.getObject(flinkColumnToHologresColumn.get(index)))
                                    .getArray();
            StringData[] arrStringObject = new StringData[values.length];
            for (int i = 0; i < values.length; ++i) {
                arrStringObject[i] = StringData.fromString(values[i]);
            }
            return new GenericArrayData(arrStringObject);
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }
}
