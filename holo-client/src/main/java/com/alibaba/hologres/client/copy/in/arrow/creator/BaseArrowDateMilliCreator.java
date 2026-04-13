package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.DateMilliVector;
import org.postgresql.jdbc.TimestampUtil;

/** Arrow column vector creator for date milli. */
public class BaseArrowDateMilliCreator extends AbstractArrowVectorCreator {

    protected final DateMilliVector dateMilliVector;

    public BaseArrowDateMilliCreator(DateMilliVector dateMilliVector) {
        super(dateMilliVector);
        this.dateMilliVector = dateMilliVector;
    }

    public void setMilliSeconds(int rowId, long value) {
        dateMilliVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            dateMilliVector.setNull(rowId);
        } else {
            setMilliSeconds(rowId, TimestampUtil.timestampToMillisecond(value, "timestamptz"));
        }
    }
}
