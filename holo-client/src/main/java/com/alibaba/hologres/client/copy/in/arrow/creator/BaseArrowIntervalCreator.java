package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.postgresql.util.PGInterval;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Arrow column vector creator for interval type (fixed_size_binary[16]). */
public class BaseArrowIntervalCreator extends AbstractArrowVectorCreator {
    protected final FixedSizeBinaryVector fixedSizeBinaryVector;

    public BaseArrowIntervalCreator(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
        this.fixedSizeBinaryVector = fixedSizeBinaryVector;
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            fixedSizeBinaryVector.setNull(rowId);
        } else {
            PGInterval interval;
            if (value instanceof PGInterval) {
                interval = (PGInterval) value;
            } else if (value instanceof String) {
                try {
                    interval = new PGInterval((String) value);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse interval: " + value, e);
                }
            } else {
                throw new RuntimeException(
                        "Unsupported type for interval: " + value.getClass().getName());
            }

            // PostgreSQL interval binary format (little-endian):
            // 8 bytes: time in microseconds
            // 4 bytes: days
            // 4 bytes: months
            long microseconds =
                    interval.getHours() * 3600_000_000L
                            + interval.getMinutes() * 60_000_000L
                            + (long) (interval.getSeconds() * 1_000_000.0);
            int days = interval.getDays();
            int months = interval.getYears() * 12 + interval.getMonths();

            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(0, microseconds);
            buffer.putInt(8, days);
            buffer.putInt(12, months);
            fixedSizeBinaryVector.setSafe(rowId, buffer.array());
        }
    }
}
