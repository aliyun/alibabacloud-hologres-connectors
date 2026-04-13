package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.TimeStampMicroVector;
import org.postgresql.jdbc.TimestampUtil;

public class BaseArrowTimeStampMicroCreator extends AbstractArrowVectorCreator {
    protected final TimeStampMicroVector timeStampMicroVector;

    public BaseArrowTimeStampMicroCreator(TimeStampMicroVector vector) {
        super(vector);
        this.timeStampMicroVector = vector;
    }

    public void setMicroSeconds(int rowId, long value) {
        timeStampMicroVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            timeStampMicroVector.setNull(rowId);
        } else {
            setMicroSeconds(rowId, TimestampUtil.timestampToMicroSecond(value, "timestamp", false));
        }
    }
}
