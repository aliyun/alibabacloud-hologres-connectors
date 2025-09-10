package com.alibaba.hologres.connector.flink.sink.v1.multi;

import java.io.Serializable;
import java.util.Map;

/** The user converts their own data type (e.g., JSON) to the Holo Record type. */
public interface HoloRecordConverter<T> extends Serializable {
    Map<String, Object> convert(T record);
}
