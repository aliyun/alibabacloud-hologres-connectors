package com.alibaba.hologres.client.ddl;

/** 语句关键词. */
public class StatementKeywords {
    // 关键词
    public static final String BEGIN = "BEGIN";
    public static final String COMMIT = "COMMIT";
    public static final String CREATE = "CREATE";
    public static final String TABLE = "TABLE";
    public static final String IF_EXISTS = "IF EXISTS";
    public static final String IF_NOT_EXISTS = "IF NOT EXISTS";
    public static final String NOT_NULL = "NOT NULL";
    public static final String PRIMARY_KEY = "PRIMARY KEY";

    // 常用分隔符
    public static final String SEMICOLON = ";";
    public static final String SPACE = " ";
    public static final String LEFT_BRACKET = "(";
    public static final String RIGHT_BRACKET = ")";
    public static final String COMMA = ",";
    public static final String SQUARE_BRACKETS = "[]";
    public static final String DOUBLE_QUOTES = "\"";

    // 属性键名
    public static final String PARTITION_BY = "PARTITION BY";
    public static final String LIST = "LIST";
    public static final String LIFECYCLE = "LIFECYCLE";

    // 默认表空间
    public static final String PG_DEFAULT = "pg_default";

    // call procedure
    public static final String CALL_PROCEDURE = "CALL SET_TABLE_PROPERTY";

    // 分区
    public static final String PARTITION = "PARTITION OF";
    public static final String PARTITION_FOR = "FOR VALUES ('";
    public static final String PARTITION_FOR_IN = "FOR VALUES IN";

    // servername
    public static final String SERVER = "SERVER";
    // option
    public static final String OPTIONS = "OPTIONS";
    // 授权
    public static final String GRANT = "GRANT";
    public static final String PUBLIC = "PUBLIC";
    // alter
    public static final String ADD = "ADD";
    public static final String COLUMN = "COLUMN";
    // insert
    public static final String INSERT_INTO = "INSERT INTO";
    public static final String FROM = "FROM";
}
