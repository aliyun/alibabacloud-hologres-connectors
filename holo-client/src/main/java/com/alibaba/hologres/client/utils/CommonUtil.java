package com.alibaba.hologres.client.utils;

import java.util.Iterator;

/**
 * Created by liangmei.gl.
 * Date: 2020-12-23 11:18
 **/
public class CommonUtil {

	public static boolean isEmpty(CharSequence cs) {
		return cs == null || cs.length() == 0;
	}

	public static boolean isEmpty(Object[] cs) {
		return cs == null || cs.length == 0;
	}

	public static String join(Object[] array, String separator) {
		return array == null ? null : join((Object[]) array, separator, 0, array.length);
	}

	public static boolean isNotEmpty(Object[] cs) {
		return !isEmpty(cs);
	}

	public static boolean isNotEmpty(CharSequence cs) {
		return !isEmpty(cs);
	}

	public static String join(Object[] array, String separator, int startIndex, int endIndex) {
		if (array == null) {
			return null;
		} else {
			if (separator == null) {
				separator = "";
			}

			int noOfItems = endIndex - startIndex;
			if (noOfItems <= 0) {
				return "";
			} else {
				StringBuilder buf = newStringBuilder(noOfItems);
				if (array[startIndex] != null) {
					buf.append(array[startIndex]);
				}

				for (int i = startIndex + 1; i < endIndex; ++i) {
					buf.append(separator);
					if (array[i] != null) {
						buf.append(array[i]);
					}
				}

				return buf.toString();
			}
		}
	}

	private static StringBuilder newStringBuilder(int noOfItems) {
		return new StringBuilder(noOfItems * 16);
	}

	public static String join(Iterable<?> iterable, String separator) {
		return iterable == null ? null : join(iterable.iterator(), separator);
	}

	public static String join(Iterator<?> iterator, String separator) {
		if (iterator == null) {
			return null;
		} else if (!iterator.hasNext()) {
			return "";
		} else {
			Object first = iterator.next();
			if (!iterator.hasNext()) {
				String result = toString(first);
				return result;
			} else {
				StringBuilder buf = new StringBuilder(256);
				if (first != null) {
					buf.append(first);
				}

				while (iterator.hasNext()) {
					if (separator != null) {
						buf.append(separator);
					}

					Object obj = iterator.next();
					if (obj != null) {
						buf.append(obj);
					}
				}

				return buf.toString();
			}
		}
	}

	public static String toString(Object obj) {
		return obj == null ? "" : obj.toString();
	}
}
