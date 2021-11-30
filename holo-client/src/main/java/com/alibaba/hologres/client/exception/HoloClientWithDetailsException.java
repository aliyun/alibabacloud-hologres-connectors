/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

import com.alibaba.hologres.client.model.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * A subclass of {@link com.alibaba.hologres.client.exception.HoloClientException}.
 * It is thrown when we have more information about which records were causing which
 * exceptions.
 */
public class HoloClientWithDetailsException extends HoloClientException {

	List<Record> failedList;
	List<HoloClientException> exceptionList;

	public HoloClientWithDetailsException(HoloClientException e) {
		super(e.getCode(), e.getMessage(), e.getCause());
	}

	public HoloClientWithDetailsException(ExceptionCode code, String msg, Record record) {
		super(code, msg);
		HoloClientException e = new HoloClientException(code, msg);
		add(record, e);
	}

	public void add(List<Record> failList, HoloClientException e) {
		for (Record record : failList) {
			add(record, e);
		}
	}

	public void add(Record record, HoloClientException e) {
		if (failedList == null) {
			failedList = new ArrayList<>();
			exceptionList = new ArrayList<>();
		}
		failedList.add(record);
		exceptionList.add(e);
	}

	public int size() {
		return failedList == null ? 0 : failedList.size();
	}

	public Record getFailRecord(int index) {
		return failedList.get(index);
	}

	public HoloClientException getException(int index) {

		return exceptionList.get(index);

	}

	public HoloClientException getException() {
		return exceptionList.get(0);
	}

	public HoloClientWithDetailsException merge(HoloClientWithDetailsException other) {
		this.failedList.addAll(other.failedList);
		this.exceptionList.addAll(other.exceptionList);
		return this;
	}

	@Override
	public String getLocalizedMessage() {
		return getMessage();
	}

	@Override
	public String getMessage() {
		if (size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("failed records " + size() + ", first:" + getFailRecord(0) + ",first err:" + getException(0).getMessage());
			return sb.toString();
		} else {
			return super.getMessage();
		}
	}
}
