package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.FixedSizeBinaryVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Time;

/** Arrow column vector accessor for time micro. */
public class BaseArrowFixedSizeBinaryAccessor extends AbstractArrowVectorAccessor {
    protected final FixedSizeBinaryVector fixedSizeBinaryVector;

    public BaseArrowFixedSizeBinaryAccessor(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
        this.fixedSizeBinaryVector = fixedSizeBinaryVector;
    }

    public byte[] getBinary(int rowId) {
        return this.fixedSizeBinaryVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        long time =
                ByteBuffer.wrap(getBinary(rowId))
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asLongBuffer()
                        .get(0);
        int zoneOffset =
                ByteBuffer.wrap(getBinary(rowId))
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asIntBuffer()
                        .get(2);
        return new Time(time / 1000L + zoneOffset * 1000L);
    }
}
