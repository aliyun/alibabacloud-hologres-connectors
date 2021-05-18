package com.alibaba.hologres.hive;

/** Column. */
public class Column {
    String name;
    int type;

    public Column(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }
}
