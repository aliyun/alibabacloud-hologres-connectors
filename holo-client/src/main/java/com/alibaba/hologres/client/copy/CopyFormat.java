package com.alibaba.hologres.client.copy;

/** enum for copy format. */
public enum CopyFormat {
    // only for copy in now
    BINARY,
    CSV,
    BINARYROW,

    // only for copy out now
    ARROW,
    ARROW_LZ4
}
