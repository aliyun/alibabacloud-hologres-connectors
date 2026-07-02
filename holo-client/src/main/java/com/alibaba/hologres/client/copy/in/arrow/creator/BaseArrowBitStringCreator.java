package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.VarBinaryVector;

/** Arrow column vector creator for bit(n) / varbit(n) type (variable-length binary). */
public class BaseArrowBitStringCreator extends AbstractArrowVectorCreator {
    protected final VarBinaryVector varBinaryVector;
    private final int precision;

    public BaseArrowBitStringCreator(VarBinaryVector varBinaryVector) {
        this(varBinaryVector, 0);
    }

    public BaseArrowBitStringCreator(VarBinaryVector varBinaryVector, int precision) {
        super(varBinaryVector);
        this.varBinaryVector = varBinaryVector;
        this.precision = precision;
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            varBinaryVector.setNull(rowId);
        } else {
            String bitString;
            if (value instanceof String) {
                bitString = (String) value;
            } else if (value instanceof Boolean) {
                bitString = ((Boolean) value) ? "1" : "0";
            } else {
                bitString = value.toString();
            }

            // Truncate to column precision if specified
            if (precision > 0 && bitString.length() > precision) {
                bitString = bitString.substring(0, precision);
            }

            // PostgreSQL bit binary format:
            // 4 bytes: number of bits (int32, little-endian)
            // N bytes: packed bit data (MSB first, zero-padded)
            int bitCount = bitString.length();
            int byteCount = (bitCount + 7) / 8;
            byte[] result = new byte[4 + byteCount];

            // Write bit count as little-endian int32
            result[0] = (byte) (bitCount & 0xFF);
            result[1] = (byte) ((bitCount >> 8) & 0xFF);
            result[2] = (byte) ((bitCount >> 16) & 0xFF);
            result[3] = (byte) ((bitCount >> 24) & 0xFF);

            // Pack bits into bytes (MSB first)
            for (int i = 0; i < bitCount; i++) {
                if (bitString.charAt(i) == '1') {
                    result[4 + i / 8] |= (byte) (0x80 >> (i % 8));
                }
            }

            varBinaryVector.setSafe(rowId, result);
        }
    }
}
