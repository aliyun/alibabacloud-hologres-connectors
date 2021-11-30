/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.copy;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * InternalPipedOutputStream的生命周期在HoloClient内部维护.
 * Worker在执行copy时如果发现是InternalPipedOutputStream，将在完成copy后调用close(),否则不调用close()
 */
public class InternalPipedOutputStream extends PipedOutputStream {

	public InternalPipedOutputStream(PipedInputStream snk) throws IOException {
		super(snk);
	}

	public InternalPipedOutputStream() {
	}

}
