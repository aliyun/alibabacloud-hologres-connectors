/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl;

import com.alibaba.hologres.client.function.FunctionWithSQLException;
import com.alibaba.hologres.client.utils.Tuple;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * cache类.
 *
 * @param <K> key
 * @param <V> value
 */
public class Cache<K, V> {
	public static final int MODE_NO_CACHE = 1;
	public static final int MODE_ONLY_CACHE = 2;
	public static final int MODE_LOCAL_THEN_REMOTE = 0;

	static class FunctionRuntimeException extends RuntimeException {
		FunctionRuntimeException(Throwable cause) {
			super(cause);
		}
	}

	Map<K, Item> cache = new ConcurrentHashMap<>();

	FunctionWithSQLException<K, V> builder;

	private long ttl;

	class Item {
		V value;
		long createTime;

		/**
		 * true表示在第一次创建后，有再被访问过.
		 */
		boolean accessed;

		Item(V value) {
			this.value = value;
			createTime = System.currentTimeMillis();
			accessed = false;
		}

		public void setAccessed(boolean accessed) {
			this.accessed = accessed;
		}

		public boolean isAccessed() {
			return accessed;
		}

		public V get() {
			return value;
		}

		public boolean isExpire() {
			return isExpire(System.currentTimeMillis());
		}

		public boolean isExpire(long current) {
			if (ttl < 1) {
				return false;
			} else {
				return getAge(current) > ttl;
			}
		}

		/**
		 * 返回已存活时间.
		 *
		 * @param current 当前的时间
		 * @return current - createTime
		 */
		public long getAge(long current) {
			return current - createTime;
		}
	}

	public Cache() {
		this(null);
	}

	public Cache(FunctionWithSQLException<K, V> builder) {
		this(-1L, builder);
	}

	public Cache(long ttl, FunctionWithSQLException<K, V> builder) {
		this.ttl = ttl;
		this.builder = builder;
	}

	/**
	 * 获取符合所有下述条件的key列表.
	 * 1 未过期
	 * 2 accessed为ture
	 * 3 剩余存活时间少于remainLifeTime
	 *
	 * @param remainLifeTime 剩余存活时间
	 * @return key列表
	 */
	public Set<K> filterExpireKeys(long remainLifeTime) {
		long current = System.currentTimeMillis();
		return cache.entrySet().stream().filter(entry -> !entry.getValue().isExpire(current)
				&& entry.getValue().isAccessed()
				&& ttl - entry.getValue().getAge(current) < remainLifeTime
		).map(entry -> entry.getKey()).collect(Collectors.toSet());
	}

	/**
	 * cache item状态.
	 */
	public enum ItemState {
		EXPIRE,
		NEED_REFRESH
	}

	/**
	 * 获取符合所有下述条件的key列表.
	 * 1 未过期
	 * 2 accessed为ture
	 * 3 剩余存活时间少于remainLifeTime
	 *
	 * @param remainLifeTime 剩余存活时间
	 * @return key列表
	 */
	public Set<Tuple<ItemState, K>> filterKeys(long remainLifeTime) {
		long current = System.currentTimeMillis();
		return cache.entrySet().stream().filter(
				entry -> entry.getValue().isExpire(current)
						||
						(entry.getValue().isAccessed()
								&& ttl - entry.getValue().getAge(current) < remainLifeTime)
		).map(entry -> new Tuple<>(entry.getValue().isExpire(current) ? ItemState.EXPIRE : ItemState.NEED_REFRESH, entry.getKey())).collect(Collectors.toSet());
	}

	public void setBuilder(FunctionWithSQLException<K, V> builder) {
		this.builder = builder;
	}

	public V get(K key) throws SQLException {
		return get(key, null);
	}

	public V get(K key, FunctionWithSQLException<K, V> builder) throws SQLException {
		return get(key, builder, MODE_LOCAL_THEN_REMOTE);
	}

	/**
	 * get.
	 *
	 * @param key     主键
	 * @param builder value生成函数
	 * @param mode    MODE_LOCAL_THEN_REMOTE 先查缓存，没有命中（不存在或过期）则调用builder生成value
	 *                MODE_NO_CACHE 调用builder生成value
	 *                MODE_ONLY_CACHE 查缓存，存在且没有过期返回value，否则返回null
	 * @return 结果
	 * @throws SQLException builder失败抛的异常
	 */
	public V get(K key, FunctionWithSQLException<K, V> builder, final int mode) throws SQLException {
		final FunctionWithSQLException<K, V> internalBuilder = builder == null ? this.builder : builder;
		Item item = null;
		if (mode == MODE_ONLY_CACHE) {
			item = cache.get(key);
			if (item != null && !item.isExpire()) {
				item.setAccessed(true);
			} else {
				item = null;
			}
		} else {
			try {
				item = cache.compute(key, (k, old) -> {
					if (MODE_NO_CACHE == mode || old == null || old.isExpire()) {
						try {
							V value = internalBuilder.apply(k);
							if (value != null) {
								return new Item(value);
							} else {
								return null;
							}
						} catch (SQLException throwables) {
							throw new FunctionRuntimeException(throwables);
						}
					} else {
						old.setAccessed(true);
						return old;
					}
				});
			} catch (FunctionRuntimeException e) {
				Throwable cause = e.getCause();
				if (cause instanceof SQLException) {
					throw (SQLException) cause;
				} else {
					throw new SQLException(cause);
				}
			}
		}
		if (item == null) {
			return null;
		} else {
			return item.get();
		}
	}

	public void put(K key, V value) {
		if (value != null) {
			cache.put(key, new Item(value));
		}
	}

	public void remove(K key) {
		cache.remove(key);
	}

	public void clear() {
		cache.clear();
	}
}
