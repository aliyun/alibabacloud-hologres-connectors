/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import java.util.HashMap;
import java.util.Map;

/** Util class to quote Hologres identifier. */
public class IdentifierUtil {
    /**
     * Quote an identifier (e.g. column name, table name) if necessary. This method is different
     * from Postgres quote_ident() as it does NOT quote identifier with uppercase/lowercase mixed
     * case
     *
     * @param name the identifier to quote
     * @return the quoted result.
     */
    public static String quoteIdentifier(String name) {
        return quoteIdentifier(name, false, false);
    }

    public static String quoteIdentifier(String name, boolean isCaseSensitive) {
        return quoteIdentifier(name, isCaseSensitive, false);
    }

    public static String quoteIdentifier(String name, boolean isCaseSensitive, boolean forceQuote) {
        boolean hasLowercase = false;
        boolean hasUppercase = false;
        boolean safe = true;

        if (name == null) {
            return name;
        }

        if (forceQuote) {
            safe = false;
        } else {
            int len = name.length();
            for (int i = 0; i < len; i++) {
                char ch = name.charAt(i);
                if (ch >= 'a' && ch <= 'z') {
                    hasLowercase = true;
                } else if (ch >= 'A' && ch <= 'Z') {
                    hasUppercase = true;
                } else if (ch == '_') {
                    // nothing to do, skip
                } else if (ch >= '0' && ch <= '9') {
                    if (i == 0) {
                        // [0-9] is not allowed for first character
                        safe = false;
                    }
                } else {
                    safe = false;
                }
            }

            if (isCaseSensitive && hasUppercase) {
                safe = false;
            }
        }

        // check whether is reserved keyword or not
        if (safe) {
            StringBuffer sb = new StringBuffer();
            /*
             * Apply an ASCII-only downcasing.  We must not use tolower() since it may
             * produce the wrong translation in some locales (eg, Turkish).
             */
            for (int i = 0; i < name.length(); i++) {
                char ch = name.charAt(i);
                if (ch >= 'A' && ch <= 'Z') {
                    ch += 'a' - 'A';
                }
                sb.append(ch);
            }
            String lowerName = sb.toString();

            // check keyword
            Integer r = keywordMap.get(lowerName);
            if (r != null && r.intValue() != UNRESERVED_KEYWORD) {
                safe = false;
            }
        }

        if (safe) {
            return name;
        }

        StringBuffer sb = new StringBuffer();
        sb.append('"');
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '"') {
                sb.append('"');
            }
            sb.append(ch);
        }
        sb.append('"');
        return sb.toString();
    }

    static final int UNRESERVED_KEYWORD = 0;
    static final int RESERVED_KEYWORD = 1;
    static final int COL_NAME_KEYWORD = 2;
    static final int TYPE_FUNC_NAME_KEYWORD = 3;

    static Map<String, Integer> keywordMap;

    static {
        keywordMap = new HashMap<>();

        keywordMap.put("abort", UNRESERVED_KEYWORD);
        keywordMap.put("absolute", UNRESERVED_KEYWORD);
        keywordMap.put("access", UNRESERVED_KEYWORD);
        keywordMap.put("action", UNRESERVED_KEYWORD);
        keywordMap.put("add", UNRESERVED_KEYWORD);
        keywordMap.put("admin", UNRESERVED_KEYWORD);
        keywordMap.put("after", UNRESERVED_KEYWORD);
        keywordMap.put("aggregate", UNRESERVED_KEYWORD);
        keywordMap.put("all", RESERVED_KEYWORD);
        keywordMap.put("also", UNRESERVED_KEYWORD);
        keywordMap.put("alter", UNRESERVED_KEYWORD);
        keywordMap.put("always", UNRESERVED_KEYWORD);
        keywordMap.put("analyse", RESERVED_KEYWORD);
        keywordMap.put("analyze", RESERVED_KEYWORD);
        keywordMap.put("and", RESERVED_KEYWORD);
        keywordMap.put("any", RESERVED_KEYWORD);
        keywordMap.put("array", RESERVED_KEYWORD);
        keywordMap.put("as", RESERVED_KEYWORD);
        keywordMap.put("asc", RESERVED_KEYWORD);
        keywordMap.put("assertion", UNRESERVED_KEYWORD);
        keywordMap.put("assignment", UNRESERVED_KEYWORD);
        keywordMap.put("asymmetric", RESERVED_KEYWORD);
        keywordMap.put("at", UNRESERVED_KEYWORD);
        keywordMap.put("attach", UNRESERVED_KEYWORD);
        keywordMap.put("attribute", UNRESERVED_KEYWORD);
        keywordMap.put("authorization", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("backward", UNRESERVED_KEYWORD);
        keywordMap.put("before", UNRESERVED_KEYWORD);
        keywordMap.put("begin", UNRESERVED_KEYWORD);
        keywordMap.put("between", COL_NAME_KEYWORD);
        keywordMap.put("bigint", COL_NAME_KEYWORD);
        keywordMap.put("binary", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("bit", COL_NAME_KEYWORD);
        keywordMap.put("boolean", COL_NAME_KEYWORD);
        keywordMap.put("both", RESERVED_KEYWORD);
        keywordMap.put("by", UNRESERVED_KEYWORD);
        keywordMap.put("cache", UNRESERVED_KEYWORD);
        keywordMap.put("call", UNRESERVED_KEYWORD);
        keywordMap.put("called", UNRESERVED_KEYWORD);
        keywordMap.put("cascade", UNRESERVED_KEYWORD);
        keywordMap.put("cascaded", UNRESERVED_KEYWORD);
        keywordMap.put("case", RESERVED_KEYWORD);
        keywordMap.put("cast", RESERVED_KEYWORD);
        keywordMap.put("catalog", UNRESERVED_KEYWORD);
        keywordMap.put("chain", UNRESERVED_KEYWORD);
        keywordMap.put("char", COL_NAME_KEYWORD);
        keywordMap.put("character", COL_NAME_KEYWORD);
        keywordMap.put("characteristics", UNRESERVED_KEYWORD);
        keywordMap.put("check", RESERVED_KEYWORD);
        keywordMap.put("checkpoint", UNRESERVED_KEYWORD);
        keywordMap.put("class", UNRESERVED_KEYWORD);
        keywordMap.put("close", UNRESERVED_KEYWORD);
        keywordMap.put("cluster", UNRESERVED_KEYWORD);
        keywordMap.put("coalesce", COL_NAME_KEYWORD);
        keywordMap.put("collate", RESERVED_KEYWORD);
        keywordMap.put("collation", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("column", RESERVED_KEYWORD);
        keywordMap.put("columns", UNRESERVED_KEYWORD);
        keywordMap.put("comment", UNRESERVED_KEYWORD);
        keywordMap.put("comments", UNRESERVED_KEYWORD);
        keywordMap.put("commit", UNRESERVED_KEYWORD);
        keywordMap.put("committed", UNRESERVED_KEYWORD);
        keywordMap.put("concurrently", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("configuration", UNRESERVED_KEYWORD);
        keywordMap.put("conflict", UNRESERVED_KEYWORD);
        keywordMap.put("connection", UNRESERVED_KEYWORD);
        keywordMap.put("constraint", RESERVED_KEYWORD);
        keywordMap.put("constraints", UNRESERVED_KEYWORD);
        keywordMap.put("content", UNRESERVED_KEYWORD);
        keywordMap.put("continue", UNRESERVED_KEYWORD);
        keywordMap.put("conversion", UNRESERVED_KEYWORD);
        keywordMap.put("copy", UNRESERVED_KEYWORD);
        keywordMap.put("cost", UNRESERVED_KEYWORD);
        keywordMap.put("create", RESERVED_KEYWORD);
        keywordMap.put("cross", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("csv", UNRESERVED_KEYWORD);
        keywordMap.put("cube", UNRESERVED_KEYWORD);
        keywordMap.put("current", UNRESERVED_KEYWORD);
        keywordMap.put("current_catalog", RESERVED_KEYWORD);
        keywordMap.put("current_date", RESERVED_KEYWORD);
        keywordMap.put("current_role", RESERVED_KEYWORD);
        keywordMap.put("current_schema", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("current_time", RESERVED_KEYWORD);
        keywordMap.put("current_timestamp", RESERVED_KEYWORD);
        keywordMap.put("current_user", RESERVED_KEYWORD);
        keywordMap.put("cursor", UNRESERVED_KEYWORD);
        keywordMap.put("cycle", UNRESERVED_KEYWORD);
        keywordMap.put("data", UNRESERVED_KEYWORD);
        keywordMap.put("database", UNRESERVED_KEYWORD);
        keywordMap.put("day", UNRESERVED_KEYWORD);
        keywordMap.put("deallocate", UNRESERVED_KEYWORD);
        keywordMap.put("dec", COL_NAME_KEYWORD);
        keywordMap.put("decimal", COL_NAME_KEYWORD);
        keywordMap.put("declare", UNRESERVED_KEYWORD);
        keywordMap.put("default", RESERVED_KEYWORD);
        keywordMap.put("defaults", UNRESERVED_KEYWORD);
        keywordMap.put("deferrable", RESERVED_KEYWORD);
        keywordMap.put("deferred", UNRESERVED_KEYWORD);
        keywordMap.put("definer", UNRESERVED_KEYWORD);
        keywordMap.put("delete", UNRESERVED_KEYWORD);
        keywordMap.put("delimiter", UNRESERVED_KEYWORD);
        keywordMap.put("delimiters", UNRESERVED_KEYWORD);
        keywordMap.put("depends", UNRESERVED_KEYWORD);
        keywordMap.put("desc", RESERVED_KEYWORD);
        keywordMap.put("detach", UNRESERVED_KEYWORD);
        keywordMap.put("dictionary", UNRESERVED_KEYWORD);
        keywordMap.put("disable", UNRESERVED_KEYWORD);
        keywordMap.put("discard", UNRESERVED_KEYWORD);
        keywordMap.put("distinct", RESERVED_KEYWORD);
        keywordMap.put("do", RESERVED_KEYWORD);
        keywordMap.put("document", UNRESERVED_KEYWORD);
        keywordMap.put("domain", UNRESERVED_KEYWORD);
        keywordMap.put("double", UNRESERVED_KEYWORD);
        keywordMap.put("drop", UNRESERVED_KEYWORD);
        keywordMap.put("each", UNRESERVED_KEYWORD);
        keywordMap.put("else", RESERVED_KEYWORD);
        keywordMap.put("enable", UNRESERVED_KEYWORD);
        keywordMap.put("encoding", UNRESERVED_KEYWORD);
        keywordMap.put("encrypted", UNRESERVED_KEYWORD);
        keywordMap.put("end", RESERVED_KEYWORD);
        keywordMap.put("enum", UNRESERVED_KEYWORD);
        keywordMap.put("escape", UNRESERVED_KEYWORD);
        keywordMap.put("event", UNRESERVED_KEYWORD);
        keywordMap.put("except", RESERVED_KEYWORD);
        keywordMap.put("exclude", UNRESERVED_KEYWORD);
        keywordMap.put("excluding", UNRESERVED_KEYWORD);
        keywordMap.put("exclusive", UNRESERVED_KEYWORD);
        keywordMap.put("execute", UNRESERVED_KEYWORD);
        keywordMap.put("exists", COL_NAME_KEYWORD);
        keywordMap.put("explain", UNRESERVED_KEYWORD);
        keywordMap.put("extension", UNRESERVED_KEYWORD);
        keywordMap.put("external", UNRESERVED_KEYWORD);
        keywordMap.put("extract", COL_NAME_KEYWORD);
        keywordMap.put("false", RESERVED_KEYWORD);
        keywordMap.put("family", UNRESERVED_KEYWORD);
        keywordMap.put("fetch", RESERVED_KEYWORD);
        keywordMap.put("filter", UNRESERVED_KEYWORD);
        keywordMap.put("first", UNRESERVED_KEYWORD);
        keywordMap.put("float", COL_NAME_KEYWORD);
        keywordMap.put("following", UNRESERVED_KEYWORD);
        keywordMap.put("for", RESERVED_KEYWORD);
        keywordMap.put("force", UNRESERVED_KEYWORD);
        keywordMap.put("foreign", RESERVED_KEYWORD);
        keywordMap.put("forward", UNRESERVED_KEYWORD);
        keywordMap.put("freeze", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("from", RESERVED_KEYWORD);
        keywordMap.put("full", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("function", UNRESERVED_KEYWORD);
        keywordMap.put("functions", UNRESERVED_KEYWORD);
        keywordMap.put("generated", UNRESERVED_KEYWORD);
        keywordMap.put("global", UNRESERVED_KEYWORD);
        keywordMap.put("grant", RESERVED_KEYWORD);
        keywordMap.put("granted", UNRESERVED_KEYWORD);
        keywordMap.put("greatest", COL_NAME_KEYWORD);
        keywordMap.put("group", RESERVED_KEYWORD);
        keywordMap.put("grouping", COL_NAME_KEYWORD);
        keywordMap.put("groups", UNRESERVED_KEYWORD);
        keywordMap.put("handler", UNRESERVED_KEYWORD);
        keywordMap.put("having", RESERVED_KEYWORD);
        keywordMap.put("header", UNRESERVED_KEYWORD);
        keywordMap.put("hold", UNRESERVED_KEYWORD);
        keywordMap.put("hour", UNRESERVED_KEYWORD);
        keywordMap.put("identity", UNRESERVED_KEYWORD);
        keywordMap.put("if", UNRESERVED_KEYWORD);
        keywordMap.put("ilike", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("immediate", UNRESERVED_KEYWORD);
        keywordMap.put("immutable", UNRESERVED_KEYWORD);
        keywordMap.put("implicit", UNRESERVED_KEYWORD);
        keywordMap.put("import", UNRESERVED_KEYWORD);
        keywordMap.put("in", RESERVED_KEYWORD);
        keywordMap.put("include", UNRESERVED_KEYWORD);
        keywordMap.put("including", UNRESERVED_KEYWORD);
        keywordMap.put("increment", UNRESERVED_KEYWORD);
        keywordMap.put("index", UNRESERVED_KEYWORD);
        keywordMap.put("indexes", UNRESERVED_KEYWORD);
        keywordMap.put("inherit", UNRESERVED_KEYWORD);
        keywordMap.put("inherits", UNRESERVED_KEYWORD);
        keywordMap.put("initially", RESERVED_KEYWORD);
        keywordMap.put("inline", UNRESERVED_KEYWORD);
        keywordMap.put("inner", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("inout", COL_NAME_KEYWORD);
        keywordMap.put("input", UNRESERVED_KEYWORD);
        keywordMap.put("insensitive", UNRESERVED_KEYWORD);
        keywordMap.put("insert", UNRESERVED_KEYWORD);
        keywordMap.put("instead", UNRESERVED_KEYWORD);
        keywordMap.put("int", COL_NAME_KEYWORD);
        keywordMap.put("integer", COL_NAME_KEYWORD);
        keywordMap.put("intersect", RESERVED_KEYWORD);
        keywordMap.put("interval", COL_NAME_KEYWORD);
        keywordMap.put("into", RESERVED_KEYWORD);
        keywordMap.put("invoker", UNRESERVED_KEYWORD);
        keywordMap.put("is", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("isnull", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("isolation", UNRESERVED_KEYWORD);
        keywordMap.put("join", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("key", UNRESERVED_KEYWORD);
        keywordMap.put("label", UNRESERVED_KEYWORD);
        keywordMap.put("language", UNRESERVED_KEYWORD);
        keywordMap.put("large", UNRESERVED_KEYWORD);
        keywordMap.put("last", UNRESERVED_KEYWORD);
        keywordMap.put("lateral", RESERVED_KEYWORD);
        keywordMap.put("leading", RESERVED_KEYWORD);
        keywordMap.put("leakproof", UNRESERVED_KEYWORD);
        keywordMap.put("least", COL_NAME_KEYWORD);
        keywordMap.put("left", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("level", UNRESERVED_KEYWORD);
        keywordMap.put("like", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("limit", RESERVED_KEYWORD);
        keywordMap.put("listen", UNRESERVED_KEYWORD);
        keywordMap.put("load", UNRESERVED_KEYWORD);
        keywordMap.put("local", UNRESERVED_KEYWORD);
        keywordMap.put("localtime", RESERVED_KEYWORD);
        keywordMap.put("localtimestamp", RESERVED_KEYWORD);
        keywordMap.put("location", UNRESERVED_KEYWORD);
        keywordMap.put("lock", UNRESERVED_KEYWORD);
        keywordMap.put("locked", UNRESERVED_KEYWORD);
        keywordMap.put("logged", UNRESERVED_KEYWORD);
        keywordMap.put("mapping", UNRESERVED_KEYWORD);
        keywordMap.put("match", UNRESERVED_KEYWORD);
        keywordMap.put("materialized", UNRESERVED_KEYWORD);
        keywordMap.put("maxvalue", UNRESERVED_KEYWORD);
        keywordMap.put("method", UNRESERVED_KEYWORD);
        keywordMap.put("minute", UNRESERVED_KEYWORD);
        keywordMap.put("minvalue", UNRESERVED_KEYWORD);
        keywordMap.put("mode", UNRESERVED_KEYWORD);
        keywordMap.put("month", UNRESERVED_KEYWORD);
        keywordMap.put("move", UNRESERVED_KEYWORD);
        keywordMap.put("name", UNRESERVED_KEYWORD);
        keywordMap.put("names", UNRESERVED_KEYWORD);
        keywordMap.put("national", COL_NAME_KEYWORD);
        keywordMap.put("natural", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("nchar", COL_NAME_KEYWORD);
        keywordMap.put("new", UNRESERVED_KEYWORD);
        keywordMap.put("next", UNRESERVED_KEYWORD);
        keywordMap.put("no", UNRESERVED_KEYWORD);
        keywordMap.put("none", COL_NAME_KEYWORD);
        keywordMap.put("not", RESERVED_KEYWORD);
        keywordMap.put("nothing", UNRESERVED_KEYWORD);
        keywordMap.put("notify", UNRESERVED_KEYWORD);
        keywordMap.put("notnull", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("nowait", UNRESERVED_KEYWORD);
        keywordMap.put("null", RESERVED_KEYWORD);
        keywordMap.put("nullif", COL_NAME_KEYWORD);
        keywordMap.put("nulls", UNRESERVED_KEYWORD);
        keywordMap.put("numeric", COL_NAME_KEYWORD);
        keywordMap.put("object", UNRESERVED_KEYWORD);
        keywordMap.put("of", UNRESERVED_KEYWORD);
        keywordMap.put("off", UNRESERVED_KEYWORD);
        keywordMap.put("offset", RESERVED_KEYWORD);
        keywordMap.put("oids", UNRESERVED_KEYWORD);
        keywordMap.put("old", UNRESERVED_KEYWORD);
        keywordMap.put("on", RESERVED_KEYWORD);
        keywordMap.put("only", RESERVED_KEYWORD);
        keywordMap.put("operator", UNRESERVED_KEYWORD);
        keywordMap.put("option", UNRESERVED_KEYWORD);
        keywordMap.put("options", UNRESERVED_KEYWORD);
        keywordMap.put("or", RESERVED_KEYWORD);
        keywordMap.put("order", RESERVED_KEYWORD);
        keywordMap.put("ordinality", UNRESERVED_KEYWORD);
        keywordMap.put("others", UNRESERVED_KEYWORD);
        keywordMap.put("out", COL_NAME_KEYWORD);
        keywordMap.put("outer", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("over", UNRESERVED_KEYWORD);
        keywordMap.put("overlaps", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("overlay", COL_NAME_KEYWORD);
        keywordMap.put("overriding", UNRESERVED_KEYWORD);
        keywordMap.put("owned", UNRESERVED_KEYWORD);
        keywordMap.put("owner", UNRESERVED_KEYWORD);
        keywordMap.put("parallel", UNRESERVED_KEYWORD);
        keywordMap.put("parser", UNRESERVED_KEYWORD);
        keywordMap.put("partial", UNRESERVED_KEYWORD);
        keywordMap.put("partition", UNRESERVED_KEYWORD);
        keywordMap.put("passing", UNRESERVED_KEYWORD);
        keywordMap.put("password", UNRESERVED_KEYWORD);
        keywordMap.put("placing", RESERVED_KEYWORD);
        keywordMap.put("plans", UNRESERVED_KEYWORD);
        keywordMap.put("policy", UNRESERVED_KEYWORD);
        keywordMap.put("position", COL_NAME_KEYWORD);
        keywordMap.put("preceding", UNRESERVED_KEYWORD);
        keywordMap.put("precision", COL_NAME_KEYWORD);
        keywordMap.put("prepare", UNRESERVED_KEYWORD);
        keywordMap.put("prepared", UNRESERVED_KEYWORD);
        keywordMap.put("preserve", UNRESERVED_KEYWORD);
        keywordMap.put("primary", RESERVED_KEYWORD);
        keywordMap.put("prior", UNRESERVED_KEYWORD);
        keywordMap.put("privileges", UNRESERVED_KEYWORD);
        keywordMap.put("procedural", UNRESERVED_KEYWORD);
        keywordMap.put("procedure", UNRESERVED_KEYWORD);
        keywordMap.put("procedures", UNRESERVED_KEYWORD);
        keywordMap.put("program", UNRESERVED_KEYWORD);
        keywordMap.put("publication", UNRESERVED_KEYWORD);
        keywordMap.put("quote", UNRESERVED_KEYWORD);
        keywordMap.put("range", UNRESERVED_KEYWORD);
        keywordMap.put("read", UNRESERVED_KEYWORD);
        keywordMap.put("real", COL_NAME_KEYWORD);
        keywordMap.put("reassign", UNRESERVED_KEYWORD);
        keywordMap.put("recheck", UNRESERVED_KEYWORD);
        keywordMap.put("recursive", UNRESERVED_KEYWORD);
        keywordMap.put("ref", UNRESERVED_KEYWORD);
        keywordMap.put("references", RESERVED_KEYWORD);
        keywordMap.put("referencing", UNRESERVED_KEYWORD);
        keywordMap.put("refresh", UNRESERVED_KEYWORD);
        keywordMap.put("reindex", UNRESERVED_KEYWORD);
        keywordMap.put("relative", UNRESERVED_KEYWORD);
        keywordMap.put("release", UNRESERVED_KEYWORD);
        keywordMap.put("rename", UNRESERVED_KEYWORD);
        keywordMap.put("repeatable", UNRESERVED_KEYWORD);
        keywordMap.put("replace", UNRESERVED_KEYWORD);
        keywordMap.put("replica", UNRESERVED_KEYWORD);
        keywordMap.put("reset", UNRESERVED_KEYWORD);
        keywordMap.put("restart", UNRESERVED_KEYWORD);
        keywordMap.put("restrict", UNRESERVED_KEYWORD);
        keywordMap.put("returning", RESERVED_KEYWORD);
        keywordMap.put("returns", UNRESERVED_KEYWORD);
        keywordMap.put("revoke", UNRESERVED_KEYWORD);
        keywordMap.put("right", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("role", UNRESERVED_KEYWORD);
        keywordMap.put("rollback", UNRESERVED_KEYWORD);
        keywordMap.put("rollup", UNRESERVED_KEYWORD);
        keywordMap.put("routine", UNRESERVED_KEYWORD);
        keywordMap.put("routines", UNRESERVED_KEYWORD);
        keywordMap.put("row", COL_NAME_KEYWORD);
        keywordMap.put("rows", UNRESERVED_KEYWORD);
        keywordMap.put("rule", UNRESERVED_KEYWORD);
        keywordMap.put("savepoint", UNRESERVED_KEYWORD);
        keywordMap.put("schema", UNRESERVED_KEYWORD);
        keywordMap.put("schemas", UNRESERVED_KEYWORD);
        keywordMap.put("scroll", UNRESERVED_KEYWORD);
        keywordMap.put("search", UNRESERVED_KEYWORD);
        keywordMap.put("second", UNRESERVED_KEYWORD);
        keywordMap.put("security", UNRESERVED_KEYWORD);
        keywordMap.put("select", RESERVED_KEYWORD);
        keywordMap.put("sequence", UNRESERVED_KEYWORD);
        keywordMap.put("sequences", UNRESERVED_KEYWORD);
        keywordMap.put("serializable", UNRESERVED_KEYWORD);
        keywordMap.put("server", UNRESERVED_KEYWORD);
        keywordMap.put("session", UNRESERVED_KEYWORD);
        keywordMap.put("session_user", RESERVED_KEYWORD);
        keywordMap.put("set", UNRESERVED_KEYWORD);
        keywordMap.put("setof", COL_NAME_KEYWORD);
        keywordMap.put("sets", UNRESERVED_KEYWORD);
        keywordMap.put("share", UNRESERVED_KEYWORD);
        keywordMap.put("show", UNRESERVED_KEYWORD);
        keywordMap.put("similar", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("simple", UNRESERVED_KEYWORD);
        keywordMap.put("skip", UNRESERVED_KEYWORD);
        keywordMap.put("smallint", COL_NAME_KEYWORD);
        keywordMap.put("snapshot", UNRESERVED_KEYWORD);
        keywordMap.put("some", RESERVED_KEYWORD);
        keywordMap.put("sql", UNRESERVED_KEYWORD);
        keywordMap.put("stable", UNRESERVED_KEYWORD);
        keywordMap.put("standalone", UNRESERVED_KEYWORD);
        keywordMap.put("start", UNRESERVED_KEYWORD);
        keywordMap.put("statement", UNRESERVED_KEYWORD);
        keywordMap.put("statistics", UNRESERVED_KEYWORD);
        keywordMap.put("stdin", UNRESERVED_KEYWORD);
        keywordMap.put("stdout", UNRESERVED_KEYWORD);
        keywordMap.put("storage", UNRESERVED_KEYWORD);
        keywordMap.put("strict", UNRESERVED_KEYWORD);
        keywordMap.put("strip", UNRESERVED_KEYWORD);
        keywordMap.put("subscription", UNRESERVED_KEYWORD);
        keywordMap.put("substring", COL_NAME_KEYWORD);
        keywordMap.put("symmetric", RESERVED_KEYWORD);
        keywordMap.put("sysid", UNRESERVED_KEYWORD);
        keywordMap.put("system", UNRESERVED_KEYWORD);
        keywordMap.put("table", RESERVED_KEYWORD);
        keywordMap.put("tables", UNRESERVED_KEYWORD);
        keywordMap.put("tablesample", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("tablespace", UNRESERVED_KEYWORD);
        keywordMap.put("temp", UNRESERVED_KEYWORD);
        keywordMap.put("template", UNRESERVED_KEYWORD);
        keywordMap.put("temporary", UNRESERVED_KEYWORD);
        keywordMap.put("text", UNRESERVED_KEYWORD);
        keywordMap.put("then", RESERVED_KEYWORD);
        keywordMap.put("ties", UNRESERVED_KEYWORD);
        keywordMap.put("time", COL_NAME_KEYWORD);
        keywordMap.put("timestamp", COL_NAME_KEYWORD);
        keywordMap.put("to", RESERVED_KEYWORD);
        keywordMap.put("trailing", RESERVED_KEYWORD);
        keywordMap.put("transaction", UNRESERVED_KEYWORD);
        keywordMap.put("transform", UNRESERVED_KEYWORD);
        keywordMap.put("treat", COL_NAME_KEYWORD);
        keywordMap.put("trigger", UNRESERVED_KEYWORD);
        keywordMap.put("trim", COL_NAME_KEYWORD);
        keywordMap.put("true", RESERVED_KEYWORD);
        keywordMap.put("truncate", UNRESERVED_KEYWORD);
        keywordMap.put("trusted", UNRESERVED_KEYWORD);
        keywordMap.put("type", UNRESERVED_KEYWORD);
        keywordMap.put("types", UNRESERVED_KEYWORD);
        keywordMap.put("unbounded", UNRESERVED_KEYWORD);
        keywordMap.put("uncommitted", UNRESERVED_KEYWORD);
        keywordMap.put("unencrypted", UNRESERVED_KEYWORD);
        keywordMap.put("union", RESERVED_KEYWORD);
        keywordMap.put("unique", RESERVED_KEYWORD);
        keywordMap.put("unknown", UNRESERVED_KEYWORD);
        keywordMap.put("unlisten", UNRESERVED_KEYWORD);
        keywordMap.put("unlogged", UNRESERVED_KEYWORD);
        keywordMap.put("until", UNRESERVED_KEYWORD);
        keywordMap.put("update", UNRESERVED_KEYWORD);
        keywordMap.put("user", RESERVED_KEYWORD);
        keywordMap.put("using", RESERVED_KEYWORD);
        keywordMap.put("vacuum", UNRESERVED_KEYWORD);
        keywordMap.put("valid", UNRESERVED_KEYWORD);
        keywordMap.put("validate", UNRESERVED_KEYWORD);
        keywordMap.put("validator", UNRESERVED_KEYWORD);
        keywordMap.put("value", UNRESERVED_KEYWORD);
        keywordMap.put("values", COL_NAME_KEYWORD);
        keywordMap.put("varchar", COL_NAME_KEYWORD);
        keywordMap.put("variadic", RESERVED_KEYWORD);
        keywordMap.put("varying", UNRESERVED_KEYWORD);
        keywordMap.put("verbose", TYPE_FUNC_NAME_KEYWORD);
        keywordMap.put("version", UNRESERVED_KEYWORD);
        keywordMap.put("view", UNRESERVED_KEYWORD);
        keywordMap.put("views", UNRESERVED_KEYWORD);
        keywordMap.put("volatile", UNRESERVED_KEYWORD);
        keywordMap.put("when", RESERVED_KEYWORD);
        keywordMap.put("where", RESERVED_KEYWORD);
        keywordMap.put("whitespace", UNRESERVED_KEYWORD);
        keywordMap.put("window", RESERVED_KEYWORD);
        keywordMap.put("with", RESERVED_KEYWORD);
        keywordMap.put("within", UNRESERVED_KEYWORD);
        keywordMap.put("without", UNRESERVED_KEYWORD);
        keywordMap.put("work", UNRESERVED_KEYWORD);
        keywordMap.put("wrapper", UNRESERVED_KEYWORD);
        keywordMap.put("write", UNRESERVED_KEYWORD);
        keywordMap.put("xml", UNRESERVED_KEYWORD);
        keywordMap.put("xmlattributes", COL_NAME_KEYWORD);
        keywordMap.put("xmlconcat", COL_NAME_KEYWORD);
        keywordMap.put("xmlelement", COL_NAME_KEYWORD);
        keywordMap.put("xmlexists", COL_NAME_KEYWORD);
        keywordMap.put("xmlforest", COL_NAME_KEYWORD);
        keywordMap.put("xmlnamespaces", COL_NAME_KEYWORD);
        keywordMap.put("xmlparse", COL_NAME_KEYWORD);
        keywordMap.put("xmlpi", COL_NAME_KEYWORD);
        keywordMap.put("xmlroot", COL_NAME_KEYWORD);
        keywordMap.put("xmlserialize", COL_NAME_KEYWORD);
        keywordMap.put("xmltable", COL_NAME_KEYWORD);
        keywordMap.put("year", UNRESERVED_KEYWORD);
        keywordMap.put("yes", UNRESERVED_KEYWORD);
        keywordMap.put("zone", UNRESERVED_KEYWORD);
    }
}
