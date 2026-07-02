package com.alibaba.hologres.client.copy.in.arrow.creator;

import org.apache.arrow.vector.VarBinaryVector;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Arrow column vector creator for inet type (variable-length binary). */
public class BaseArrowInetCreator extends AbstractArrowVectorCreator {
    protected final VarBinaryVector varBinaryVector;

    public BaseArrowInetCreator(VarBinaryVector varBinaryVector) {
        super(varBinaryVector);
        this.varBinaryVector = varBinaryVector;
    }

    @Override
    public void set(int rowId, Object value) {
        if (value == null) {
            varBinaryVector.setNull(rowId);
        } else {
            String inetStr;
            if (value instanceof String) {
                inetStr = (String) value;
            } else {
                inetStr = value.toString();
            }

            try {
                // Parse the host part (strip CIDR prefix length if present)
                String host = inetStr;
                int maskBits = -1;
                int slashIdx = inetStr.indexOf('/');
                if (slashIdx >= 0) {
                    host = inetStr.substring(0, slashIdx);
                    maskBits = Integer.parseInt(inetStr.substring(slashIdx + 1));
                }

                InetAddress addr = InetAddress.getByName(host);
                byte[] addrBytes = addr.getAddress();

                // Hologres Arrow inet binary format:
                // 1 byte: family (2=IPv4, 3=IPv6)
                // 1 byte: netmask bits
                // N bytes: raw address bytes
                int family = (addrBytes.length == 4) ? 2 : 3;
                int bits = (maskBits >= 0) ? maskBits : (addrBytes.length == 4 ? 32 : 128);

                byte[] result = new byte[2 + addrBytes.length];
                result[0] = (byte) family;
                result[1] = (byte) bits;
                System.arraycopy(addrBytes, 0, result, 2, addrBytes.length);

                varBinaryVector.setSafe(rowId, result);
            } catch (UnknownHostException e) {
                throw new RuntimeException("Failed to parse inet address: " + inetStr, e);
            }
        }
    }
}
