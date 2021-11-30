/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import com.alibaba.hologres.client.model.WriteFailStrategy;
import com.alibaba.hologres.client.model.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

/**
 * 配置加载器.
 */
public class ConfLoader {

	public static final Logger LOG = LoggerFactory.getLogger(ConfLoader.class);

	public static <T> T load(String file, T config) throws Exception {
		return load(file, null, config);
	}

	public static <T> T load(String file, String prefix, T config) throws Exception {
		return load(file, prefix, config, true);
	}

	public static <T> T load(String file, String prefix, T config, boolean throwException) throws Exception {
		try (InputStream is = new FileInputStream(file)) {
			Properties props = new Properties();
			props.load(is);
			return load(props, prefix, config, throwException);
		}
	}

	public static <T, I, O> T load(Map<I, O> props, T config) throws Exception {
		return load(props, null, config);
	}

	public static <T, I, O> T load(Map<I, O> props, String prefix, T config) throws Exception {
		return load(props, prefix, config, true);
	}

	public static <T, I, O> T load(Map<I, O> props, String prefix, T config, boolean throwException) throws Exception {
		{
			Class clazz = config.getClass();
			Field[] fields = clazz.getDeclaredFields();
			for (Map.Entry<I, O> entry : props.entrySet()) {
				String key = entry.getKey().toString();
				String value = entry.getValue().toString();
				String searchKey = key;
				if (prefix != null) {
					if (!key.startsWith(prefix)) {
						continue;
					} else {
						searchKey = key.substring(prefix.length());
					}
				}
				boolean match = false;
				for (Field field : fields) {
					if (field.getName().equalsIgnoreCase(searchKey)) {
						match = true;
						field.setAccessible(true);
						Class<?> type = field.getType();
						if (type.equals(String.class)) {
							field.set(config, value);
						} else if (type.equals(int.class)) {
							field.set(config, Integer.parseInt(value));
						} else if (type.equals(long.class)) {
							field.set(config, Long.parseLong(value));
						} else if (type.equals(boolean.class)) {
							field.set(config, Boolean.parseBoolean(value));
						} else if (WriteMode.class.equals(type)) {
							field.set(config, WriteMode.valueOf(value));
						} else if (WriteFailStrategy.class.equals(type)) {
							field.set(config, WriteFailStrategy.valueOf(value));
						} else {
							throw new Exception("invalid type " + type + " for param " + key);
						}
						if ("password".equals(key)) {
							StringBuilder sb = new StringBuilder();
							for (int i = 0; i < value.length(); ++i) {
								sb.append("*");
							}
							LOG.info("Config {}={}", key, sb.toString());
						} else {
							LOG.info("Config {}={}", key, value);
						}
					}
				}
				if (!match) {
					if (throwException) {
						throw new Exception("param " + key + " not found in " + clazz.getName());
					} else {
						LOG.warn("param {} not found in {}", key, clazz.getName());
					}
				}
			}
			return config;
		}
	}
}
