package org.postgresql.jdbc;

import org.postgresql.util.PSQLException;
/**
 * ArrayUtil.
 */

public class ArrayUtil {

	public static String arrayToString(Object elements) throws PSQLException {
		final ArrayEncoding.ArrayEncoder arraySupport = ArrayEncoding.getArrayEncoder(elements);
		final String arrayString = arraySupport.toArrayString(',', elements);
		return arrayString;
	}

	public static void reverse(byte[] array) {
		if (array != null) {
			int i = 0;

			for (int j = array.length - 1; j > i; ++i) {
				byte tmp = array[j];
				array[j] = array[i];
				array[i] = tmp;
				--j;
			}
		}
	}
}
