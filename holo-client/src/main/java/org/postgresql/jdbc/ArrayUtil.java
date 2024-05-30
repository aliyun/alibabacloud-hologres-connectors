package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.Oid;
import org.postgresql.util.PSQLException;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * ArrayUtil.
 */

public class ArrayUtil {

	public static String arrayToString(Object elements) throws PSQLException {
		if (elements instanceof PgArray) {
			PgArray array = (PgArray) elements;
			return array.toString();
		}
		final ArrayEncoding.ArrayEncoder arraySupport = ArrayEncoding.getArrayEncoder(elements);
		final String arrayString = arraySupport.toArrayString(',', elements);
		return arrayString;
	}

	public static byte[] arrayToBinary(BaseConnection conn, Object elements, String typeName) throws SQLException {
		if (elements instanceof PgArray) {
			PgArray array = (PgArray) elements;
			return array.toBytes();
		}
		final ArrayEncoding.ArrayEncoder arraySupport = ArrayEncoding.getArrayEncoder(elements);
		int oid = arraySupport.getDefaultArrayTypeOid();
		// When element is type of String[], the getDefaultArrayTypeOid geturn Oid.VARCHAR_ARRAY.
		// If typeName is _text, change the oid.
		if (oid == Oid.VARCHAR_ARRAY) {
			if ("_text".equals(typeName)) {
				oid = Oid.TEXT_ARRAY;
			}
		}
		final byte[] arrayBytes = arraySupport.toBinaryRepresentation(conn, elements, oid);
		return arrayBytes;
	}

	public static Array objectToArray(BaseConnection conn, Object obj, String typeName) throws SQLException {
		Array array = null;
		if (obj instanceof List) {
			List<?> list = (List<?>) obj;
			array = conn.createArrayOf(typeName.substring(1), list.toArray());
		} else if (obj instanceof Object[]) {
			array = conn.createArrayOf(typeName.substring(1), (Object[]) obj);
		}
		return array;
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

	public static long getArrayLength(String[] array) {
		long len = 0;
		if (array != null) {
			for (String str : array) {
				if (str != null) {
					len += str.length();
				}
			}
		}
		return len;
	}

	public static long getArrayLength(Object[] array, String typeName) {
		long len = 0;
		if (array != null) {
			switch (typeName) {
				case "_int4":
				case "_float4":
					len = array.length * 4L;
					break;
				case "_int8":
				case "_float8":
					len = array.length * 8L;
					break;
				case "_bool":
					len = array.length;
					break;
				case "_text":
					for (Object str : array) {
						if (str != null) {
							len += str.toString().length();
						}
					}
					break;
				default:
					len = 32;
			}
		}
		return len;
	}

	public static long getArrayLength(List<?> array, String typeName) {
		long len = 0;
		if (array != null) {
			switch (typeName) {
				case "_int4":
				case "_float4":
					len = array.size() * 4L;
					break;
				case "_int8":
				case "_float8":
					len = array.size() * 8L;
					break;
				case "_bool":
					len = array.size();
					break;
				case "_text":
					for (Object str : array) {
						if (str != null) {
							len += str.toString().length();
						}
					}
					break;
				default:
					len = 32;
			}
		}
		return len;
	}

	public static long getArrayLength(PgArray array) {
		long len = 0;
		if (array != null && array.toString() != null) {
			len = array.toString().length();
		} else {
			len = 1024;
		}
		return len;
	}

	public static <T> T[] arrayConcat(T[] a, T[] b) {
		T[] result = Arrays.copyOf(a, a.length + b.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}
}
