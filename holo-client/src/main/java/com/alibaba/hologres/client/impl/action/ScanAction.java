/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.action;

import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.model.RecordScanner;

import java.util.concurrent.atomic.AtomicBoolean;

/** ga. */
public class ScanAction extends AbstractAction<RecordScanner> {

    Scan scan;
    AtomicBoolean closing;

    public ScanAction(Scan scan, AtomicBoolean closing) {
        this.scan = scan;
        this.closing = closing;
    }

    public Scan getScan() {
        return scan;
    }

    public AtomicBoolean isClientClosing() {
        return closing;
    }
}
