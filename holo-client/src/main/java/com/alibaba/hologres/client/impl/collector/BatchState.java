/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.collector;

/**
 * 每个batch的状态.
 */
public enum BatchState {
	/**
	 * 还可以继续攒批.
	 */
	NotEnough(0),
	/**
	 * buffer条数达到目标值.
	 */
	SizeEnough(1),
	/**
	 * buffer大小达到目标值.
	 */
	ByteSizeEnough(2),
	/**
	 * 等待超过目标时长.
	 */
	TimeWaitEnough(3),
	/**
	 * 猜测在到达目标时长前已无法翻倍，提早提交.
	 */
	TimeCondition(4),
	/**
	 * 猜测在到达目标大小前前已无法翻倍，提早提交.
	 * 例如，目标大小为2MB，当前128条已经1.8MB，那么此时就可以直接提交，减少sql碎片
	 */
	ByteSizeCondition(5),
	/**
	 * 猜测在到达目标总大小前已无法翻倍，提早提交.
	 */
	TotalByteSizeCondition(6),
	/**
	 * 强制提交.
	 */
	Force(7),
	/**
	 * 因为批量执行失败，所以被拆成最多1条一个batch.
	 */
	RetryOneByOne(8);

	int index;

	BatchState(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}
}
