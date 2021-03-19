package com.alibaba.ververica.connectors.hologres.api;

import java.io.Serializable;

/** HologresRecordConverter. */
public interface HologresRecordConverter<IN, OUT> extends Serializable {
    OUT convertFrom(IN record);

    IN convertTo(OUT record);

    OUT convertToPrimaryKey(IN record);
}
