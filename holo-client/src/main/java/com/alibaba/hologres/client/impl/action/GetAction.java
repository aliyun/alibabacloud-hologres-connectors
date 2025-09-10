/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import com.alibaba.hologres.client.Get;

import java.util.List;

/** ga. */
public class GetAction extends AbstractAction<Void> {

    List<Get> getList;

    public GetAction(List<Get> getList) {
        this.getList = getList;
    }

    public List<Get> getGetList() {
        return getList;
    }
}
