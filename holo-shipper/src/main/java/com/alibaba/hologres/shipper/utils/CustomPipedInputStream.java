package com.alibaba.hologres.shipper.utils;

import java.io.IOException;
import java.io.PipedInputStream;

public class CustomPipedInputStream extends PipedInputStream {

    private ProcessBar.Meter meter;

    public CustomPipedInputStream(ProcessBar.Meter meter) {
        this.meter = meter;
    }
    @Override
    public int read(byte b[]) throws IOException {
        meter.mark(b.length);
        return super.read(b);
    }
}
