/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.impl.action.EmptyAction;

/**
 * EmptyAction处理类.
 */
public class EmptyActionHandler extends ActionHandler<EmptyAction> {

	private static final String NAME = "empty";

	public EmptyActionHandler(HoloConfig config) {
		super(config);
	}

	@Override
	public void handle(EmptyAction action) {

	}

	@Override
	public String getCostMsMetricName() {
		return NAME + METRIC_COST_MS;
	}
}
