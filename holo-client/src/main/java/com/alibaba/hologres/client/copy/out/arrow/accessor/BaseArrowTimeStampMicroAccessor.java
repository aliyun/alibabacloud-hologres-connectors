package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.TimeStampMicroVector;

import java.sql.Timestamp;
import java.util.TimeZone;

/** Arrow column vector accessor for timestamp micro. */
public class BaseArrowTimeStampMicroAccessor extends AbstractArrowVectorAccessor {

    protected final TimeStampMicroVector timeStampMicroVector;

    public BaseArrowTimeStampMicroAccessor(TimeStampMicroVector timeStampMicroVector) {
        super(timeStampMicroVector);
        this.timeStampMicroVector = timeStampMicroVector;
    }

    public long getMicroSeconds(int rowId) {
        return this.timeStampMicroVector.get(rowId);
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        long microseconds = getMicroSeconds(rowId);
        Timestamp timestamp =
                new Timestamp(
                        microseconds / 1000L
                                - TimeZone.getDefault().getOffset(microseconds / 1000));
        timestamp.setNanos((int) ((microseconds % 1_000_000L) * 1_000));
        return timestamp;
    }
}
