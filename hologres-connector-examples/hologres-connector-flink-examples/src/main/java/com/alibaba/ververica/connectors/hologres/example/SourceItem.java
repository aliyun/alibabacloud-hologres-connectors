package com.alibaba.ververica.connectors.hologres.example;

import java.math.BigDecimal;
import java.sql.Timestamp;

/** SourceItem. */
public class SourceItem {
    /** for example: event type. */
    public enum EventType {
        INSERT,
        DELETE
    }
    EventType eventType = EventType.INSERT;
    public long userId;
    public String userName;
    public BigDecimal price;
    public Timestamp saleTimestamp;

    public SourceItem() {}

    public SourceItem(long userId, String userName, BigDecimal price, Timestamp saleTimestamp) {
        this.userId = userId;
        this.userName = userName;
        this.price = price;
        this.saleTimestamp = saleTimestamp;
    }
}
