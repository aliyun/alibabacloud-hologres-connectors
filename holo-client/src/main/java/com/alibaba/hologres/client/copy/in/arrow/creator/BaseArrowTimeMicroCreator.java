package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.TimeMicroVector;
import org.postgresql.jdbc.TimestampUtil;

import java.io.IOException;
import java.util.TimeZone;

/** Arrow column vector creator for time micro. */
public class BaseArrowTimeMicroCreator extends AbstractArrowVectorCreator {
    public static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

    protected final TimeMicroVector timeMicroVector;

    public BaseArrowTimeMicroCreator(TimeMicroVector timeMicroVector) {
        super(timeMicroVector);
        this.timeMicroVector = timeMicroVector;
    }

    public void setMicroSeconds(int rowId, long value) {
        timeMicroVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            timeMicroVector.setNull(rowId);
        } else {
            long tVal;
            try {
                tVal = TimestampUtil.timeToMicroOfDay(value, "time").l;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            setMicroSeconds(rowId, tVal);
        }
    }
}
