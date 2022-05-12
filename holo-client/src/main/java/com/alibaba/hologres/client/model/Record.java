/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.Trace;
import org.postgresql.jdbc.ArrayUtil;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a record.
 */
public class Record implements Serializable {

	TableSchema schema;
	Object[] values;
	BitSet bitSet;
	BitSet onlyInsertColumnSet;
	List<Object> attachmentList = null;
	Put.MutationType type = Put.MutationType.INSERT;
	int shardId = -1;

	long byteSize = 0;

	/**
	 * 只在put场景下使用，存储这个Record对应的所有put的future.
	 * 例：
	 * 当前后2个Put请求的主键相同时，那么两个Put对象的Record会合并成1个，那么合并后的Record的putFutures变量将会保留2个Put对象的future
	 */
	transient List<CompletableFuture<Void>> putFutures;

	private Record(TableSchema schema,
				   Object[] values,
				   BitSet bitSet,
				   BitSet onlyInsertColumnSet,
				   List<Object> attachmentList,
				   Put.MutationType type,
				   long byteSize,
				   int shardId) {
		this.schema = schema;
		this.values = values;
		this.bitSet = bitSet;
		this.onlyInsertColumnSet = onlyInsertColumnSet;
		this.attachmentList = attachmentList;
		this.type = type;
		this.byteSize = byteSize;
		this.shardId = shardId;
	}

	public Record clone() {
		return new Record(schema,
				values.clone(),
				(BitSet) bitSet.clone(),
				(BitSet) onlyInsertColumnSet.clone(),
				null,
				type,
				byteSize,
				shardId);
	}

	Trace trace;

	public void startTrace() {
		trace = new Trace();
		trace.begin();
	}

	public void stepTrace(String name) {
		if (trace != null) {
			trace.step(name);
		}
	}

	public Trace getTrace() {
		return trace;
	}

	public Object[] getValues() {
		return values;
	}

	public Put.MutationType getType() {
		return type;
	}

	public void setType(Put.MutationType type) {
		this.type = type;
	}

	public TableSchema getSchema() {
		return schema;
	}

	public boolean isAllColumnSet() {
		return bitSet.stream().count() == schema.getColumnSchema().length;
	}

	public static Record build(TableSchema schema) {
		return new Record(schema);
	}

	public Record(TableSchema schema) {
		this.schema = schema;
		bitSet = new BitSet(schema.getColumnSchema().length);
		onlyInsertColumnSet = new BitSet(schema.getColumnSchema().length);
		values = new Object[schema.getColumnSchema().length];
	}

	public boolean isSet(int index) {
		return bitSet.get(index);
	}

	public long getByteSize() {
		return byteSize;
	}

	private long getObjByteSize(int index, Object obj) {
		if (obj == null) {
			return 4;
		}
		long ret = 0;
		Column column = schema.getColumnSchema()[index];
		switch (column.getType()) {
			case Types.BOOLEAN:
			case Types.TINYINT:
			case Types.BIT:
				ret = 1;
				break;
			case Types.SMALLINT:
				ret = 2;
				break;
			case Types.BIGINT:
			case Types.DOUBLE:
				ret = 8;
				break;
			case Types.TIMESTAMP:
			case Types.TIME_WITH_TIMEZONE:
				ret = 12;
				break;
			case Types.NUMERIC:
			case Types.DECIMAL:
				ret = 24;
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				ret = String.valueOf(obj).length();
				break;
			case Types.ARRAY:
				if (obj instanceof int[]) {
					ret = ((int[]) obj).length * 4L;
				} else if (obj instanceof long[]) {
					ret = ((long[]) obj).length * 8L;
				} else if (obj instanceof float[]) {
					ret = ((float[]) obj).length * 4L;
				} else if (obj instanceof double[]) {
					ret = ((double[]) obj).length * 8L;
				} else if (obj instanceof boolean[]) {
					ret = ((boolean[]) obj).length;
				} else if (obj instanceof String[]) {
					ret = ArrayUtil.getArrayLength((String[]) obj);
				} else if (obj instanceof Object[]) {
					ret = ArrayUtil.getArrayLength((Object[]) obj, column.getTypeName());
				} else if (obj instanceof List) {
					ret = ArrayUtil.getArrayLength((List<?>) obj, column.getTypeName());
				} else if (obj instanceof PgArray) {
					ret = ArrayUtil.getArrayLength((PgArray) obj);
				} else {
					ret = 1024;
				}
				break;
			default:
				if ("json".equalsIgnoreCase(column.getTypeName()) || "jsonb".equalsIgnoreCase(column.getTypeName())) { // json, jsonb 等类型
					ret = String.valueOf(obj).length();
					break;
				}
				if (obj instanceof PGobject) { // PGmoney 等类型
					PGobject pObj = (PGobject) obj;
					if (pObj.getValue() != null) {
						ret = pObj.getValue().length();
					}
				} else if (obj instanceof byte[]) { // RoaringBitmap, bytea 等类型
					ret = ((byte[]) obj).length;
				} else {
					ret = 4;
				}
		}
		return ret;
	}

	public void setObject(int index, Object obj) {
		Object old = values[index];
		long minus = 0L;
		if (isSet(index)) {
			minus = getObjByteSize(index, old);
		}
		long add = getObjByteSize(index, obj);
		byteSize = byteSize + add - minus;
		values[index] = obj;
		bitSet.set(index);

	}

	public Object getObject(int index) {
		return values[index];
	}

	public Object getObject(String columnName) {
		Integer index = schema.getColumnIndex(columnName);
		if (index == null) {
			throw new InvalidParameterException("can not found column named " + columnName);
		}
		return values[index];
	}

	public int[] getKeyIndex() {
		return schema.getKeyIndex();
	}

	public int getSize() {
		return schema.getColumnSchema().length;
	}

	public int getLength() {
		return (int) bitSet.stream().count();
	}

	public BitSet getBitSet() {
		return bitSet;
	}

	public BitSet getOnlyInsertColumnSet() {
		return onlyInsertColumnSet;
	}

	public void setPutFuture(CompletableFuture<Void> future) {
		if (putFutures != null) {
			throw new RuntimeException("setPutFuture should call ONLY ONCE");
		}
		putFutures = new ArrayList<>(2);
		putFutures.add(future);
	}

	/**
	 * a.merge(b).
	 * a的值被b"覆盖"（只有b set过值会覆盖到a上），a.attachmentList=a.attachmentList+b.attachmentList，b盖在a上面.
	 *
	 * @param record
	 */
	public void merge(Record record) {
		if (!schema.equals(record.schema)) {
			throw new InvalidParameterException("schema not match");
		}
		for (int i = 0; i < record.getSize(); ++i) {
			if (record.isSet(i) && !record.getOnlyInsertColumnSet().get(i)) {
				setObject(i, record.getObject(i));
			}
		}
		//merge attachment
		this.addAttachmentList(record.attachmentList);

		//merge putFutures
		if (putFutures == null) {
			if (record.putFutures != null) {
				this.putFutures = new ArrayList<>(record.putFutures);
			}
		} else {
			if (record.putFutures != null) {
				this.putFutures.addAll(record.putFutures);
			}
		}
	}

	/**
	 * a.cover(b).
	 * a把B覆盖（彻彻底底的全覆盖,不需要b的任何字段）掉，a.attachmentList=b.attachmentList+a.attachmentList，a盖在b上面.
	 *
	 * @param record
	 */
	public void cover(Record record) {
		if (!schema.equals(record.schema)) {
			throw new InvalidParameterException("schema not match");
		}
		//cover attachment
		if (record.getAttachmentList() != null) {
			List<Object> origins = record.getAttachmentList();
			if (this.getAttachmentList() != null) {
				origins.addAll(this.getAttachmentList());
			}
			this.setAttachmentList(origins);
		}
		//cover putFutures
		if (record.putFutures != null) {
			List<CompletableFuture<Void>> origins = record.putFutures;
			if (this.putFutures != null) {
				origins.addAll(this.putFutures);
			}
			this.putFutures = origins;
		}
	}

	public List<CompletableFuture<Void>> getPutFutures() {
		return putFutures;
	}

	public List<Object> getAttachmentList() {
		return attachmentList;
	}

	public void setAttachmentList(List<Object> origin) {
		this.attachmentList = origin;
	}

	public void addAttachment(Object attachment) {
		if (attachment == null) {
			return;
		}
		if (this.attachmentList == null) {
			this.attachmentList = new ArrayList<>(1);
		}
		this.attachmentList.add(attachment);
	}

	public void addAttachmentList(List<Object> list) {
		if (list == null || list.size() == 0) {
			return;
		}
		if (this.attachmentList == null) {
			this.attachmentList = new ArrayList<>(list);
		} else {
			this.attachmentList.addAll(list);
		}
	}

	public void changeToChildSchema(TableSchema schema) {
		this.schema = schema;
	}

	/**
	 * 设置Binlog Record所对应的shardId.
	 */
	public void setShardId(int shardId) {
		this.shardId = shardId;
	}

	/*
	 * -1 表示未设置.
	 *
	 * @return
	 */
	public int getShardId() {
		return shardId;
	}

	@Override
	public String toString() {
		return "Record{" +
				"schema=" + schema +
				", values=" + Arrays.toString(values) +
				", bitSet=" + bitSet +
				'}';
	}
}
