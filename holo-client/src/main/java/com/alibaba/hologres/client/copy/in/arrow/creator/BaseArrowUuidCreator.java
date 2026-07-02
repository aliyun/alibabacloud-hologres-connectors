package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.FixedSizeBinaryVector;

import java.nio.ByteBuffer;
import java.util.UUID;

/** Arrow column vector creator for UUID type (fixed_size_binary[16]). */
public class BaseArrowUuidCreator extends AbstractArrowVectorCreator {
    protected final FixedSizeBinaryVector fixedSizeBinaryVector;

    public BaseArrowUuidCreator(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
        this.fixedSizeBinaryVector = fixedSizeBinaryVector;
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            fixedSizeBinaryVector.setNull(rowId);
        } else {
            UUID uuid;
            if (value instanceof UUID) {
                uuid = (UUID) value;
            } else if (value instanceof String) {
                uuid = UUID.fromString((String) value);
            } else {
                throw new RuntimeException(
                        "Unsupported type for UUID: " + value.getClass().getName());
            }

            // UUID as 16 bytes: most significant bits first, then least significant bits
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            fixedSizeBinaryVector.setSafe(rowId, buffer.array());
        }
    }
}
