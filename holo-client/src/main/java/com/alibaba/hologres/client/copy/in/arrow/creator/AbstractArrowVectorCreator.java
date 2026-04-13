package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.ValueVector;

/** Create arrow column vector through specific subclasses. */
public abstract class AbstractArrowVectorCreator {

    private final ValueVector vector;

    public AbstractArrowVectorCreator(ValueVector vector) {
        if (vector == null) {
            throw new IllegalArgumentException("the ValueVector is null!");
        }
        this.vector = vector;
    }

    /**
     * Set the value of the underlying column vector at position {@code rowId}.
     *
     * @param rowId the position of the element.
     * @param value the value of the element.
     */
    public abstract void set(int rowId, Object value);

    public final void close() {
        vector.close();
    }
}
