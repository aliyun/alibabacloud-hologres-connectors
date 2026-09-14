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
        // 使用 floorDiv/floorMod 而非 / 和 %, 避免 1970 年之前的负值向零截断导致时间偏移,
        // 以及负数取模传入 setNanos 触发 IllegalArgumentException (与 HoloBinlogDecoder 同款修复).
        long millis = Math.floorDiv(microseconds, 1000L);
        Timestamp timestamp = new Timestamp(millis - TimeZone.getDefault().getOffset(millis));
        timestamp.setNanos((int) (Math.floorMod(microseconds, 1_000_000L) * 1_000));
        return timestamp;
    }
}
