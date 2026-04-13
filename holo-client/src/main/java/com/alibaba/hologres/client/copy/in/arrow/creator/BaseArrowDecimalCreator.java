package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Arrow column vector creator for decimal. */
public class BaseArrowDecimalCreator extends AbstractArrowVectorCreator {

    protected final DecimalVector decimalVector;
    protected final int precision;
    protected final int scale;

    public BaseArrowDecimalCreator(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.decimalVector = decimalVector;
        this.precision = precision;
        this.scale = scale;
    }

    public void setDecimal(int rowId, BigDecimal value) {
        decimalVector.setSafe(rowId, value);
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            decimalVector.setNull(rowId);
        } else {
            BigDecimal bigDecimal = (BigDecimal) value;
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.DOWN);
            setDecimal(rowId, bigDecimal);
        }
    }
}
