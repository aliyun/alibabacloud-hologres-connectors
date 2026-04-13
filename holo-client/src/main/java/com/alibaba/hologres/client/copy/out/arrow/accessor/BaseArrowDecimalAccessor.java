package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Arrow column vector accessor for decimal. */
public class BaseArrowDecimalAccessor extends AbstractArrowVectorAccessor {

    protected final DecimalVector decimalVector;
    protected final int scale;

    public BaseArrowDecimalAccessor(DecimalVector decimalVector, int scale) {
        super(decimalVector);
        this.decimalVector = decimalVector;
        this.scale = scale;
    }

    public BigDecimal getDecimal(int rowId) {
        BigDecimal bigDecimal =
                new BigDecimal(decimalVector.getObject(rowId).stripTrailingZeros().toPlainString());
        bigDecimal = bigDecimal.setScale(scale, RoundingMode.DOWN);
        return bigDecimal;
    }

    @Override
    public Object get(int rowId) {
        if (isNullAt(rowId)) {
            return null;
        }
        return getDecimal(rowId);
    }
}
