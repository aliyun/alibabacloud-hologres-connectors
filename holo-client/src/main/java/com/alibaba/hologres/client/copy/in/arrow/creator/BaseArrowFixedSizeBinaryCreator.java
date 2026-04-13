package com.alibaba.hologres.client.copy.in.arrow.creator;

import com.alibaba.hologres.client.utils.Tuple;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.postgresql.jdbc.TimestampUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Arrow column vector creator for time micro. */
public class BaseArrowFixedSizeBinaryCreator extends AbstractArrowVectorCreator {
    protected final FixedSizeBinaryVector fixedSizeBinaryVector;

    public BaseArrowFixedSizeBinaryCreator(FixedSizeBinaryVector fixedSizeBinaryVector) {
        super(fixedSizeBinaryVector);
        this.fixedSizeBinaryVector = fixedSizeBinaryVector;
    }

    public void setBinary(int rowId, byte[] value) {
        fixedSizeBinaryVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            fixedSizeBinaryVector.setNull(rowId);
        } else {
            long tVal;
            int timezone;
            try {
                Tuple<Long, Integer> tuple = TimestampUtil.timeToMicroOfDay(value, "timetz");
                tVal = tuple.l;
                timezone = tuple.r;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(0, tVal);
            buffer.putInt(8, timezone);
            buffer.putInt(12, 0);
            setBinary(rowId, buffer.array());
        }
    }
}
