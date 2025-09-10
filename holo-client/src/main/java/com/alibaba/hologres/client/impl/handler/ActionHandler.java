/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.impl.handler;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.Record;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

/**
 * Action处理类.
 *
 * @param <T> Action类型
 */
public abstract class ActionHandler<T> {

    protected static final String METRIC_COST_MS = "_cost_ms";

    public ActionHandler(HoloConfig config) {}

    public abstract void handle(T action);

    public abstract String getCostMsMetricName();

    public static void fillRecord(
            Record record, int recordIndex, ResultSet rs, int resultSetIndex, Column column)
            throws SQLException {
        switch (column.getType()) {
            case Types.SMALLINT:
                // ResultSet getShort will make null to 0.
                if (null != rs.getObject(resultSetIndex)) {
                    record.setObject(recordIndex, rs.getShort(resultSetIndex));
                    break;
                }
                record.setObject(recordIndex, rs.getObject(resultSetIndex));
                break;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                try {
                    record.setObject(recordIndex, rs.getObject(resultSetIndex));
                } catch (PSQLException e) {
                    if (Objects.equals(e.getSQLState(), PSQLState.DATA_ERROR.getState())
                            && (e.getMessage().contains("Invalid character data was found")
                                    || e.getMessage().contains("发现不合法的字元"))) {
                        // 如果字符串有非utf8字符
                        byte[] bytes = rs.getBytes(resultSetIndex);
                        record.setObject(recordIndex, new String(bytes, StandardCharsets.UTF_8));
                    } else {
                        throw e;
                    }
                }
                break;
            case Types.OTHER:
                if ("roaringbitmap".equals(column.getTypeName())) {
                    Object obj = rs.getObject(resultSetIndex);
                    if (null != obj) {
                        if (obj instanceof PGobject) {
                            String value = ((PGobject) obj).getValue();
                            if (value.startsWith("\\x")) {
                                value = value.toLowerCase();
                            }
                            final byte[] bytes = new byte[(value.length() - 2) >> 1];
                            for (int i = 2; i < value.length(); i += 2) {
                                byte highDit = (byte) (Character.digit(value.charAt(i), 16) & 0xFF);
                                byte lowDit =
                                        (byte) (Character.digit(value.charAt(i + 1), 16) & 0xFF);
                                bytes[i / 2 - 1] = (byte) (highDit << 4 | lowDit);
                            }
                            record.setObject(recordIndex, bytes);
                            break;
                        }
                    }
                }
                record.setObject(recordIndex, rs.getObject(resultSetIndex));
                break;
            default:
                record.setObject(recordIndex, rs.getObject(resultSetIndex));
        }
    }

    public static Record convertRecordColumnType(Record record) {
        Record ret = record;
        boolean needConvert = false;
        for (int keyIndex : record.getSchema().getKeyIndex()) {
            Object obj = record.getObject(keyIndex);
            int type = record.getSchema().getColumnSchema()[keyIndex].getType();
            switch (type) {
                case Types.INTEGER:
                    if (!(obj instanceof Integer)) {
                        needConvert = true;
                    }
                    break;
                case Types.BIGINT:
                    if (!(obj instanceof Long)) {
                        needConvert = true;
                    }
                    break;
                default:
                    needConvert = false;
            }
            if (needConvert) {
                break;
            }
        }
        if (needConvert) {
            ret = Record.build(record.getSchema());
            for (int keyIndex : record.getSchema().getKeyIndex()) {
                Object obj = record.getObject(keyIndex);
                int type = record.getSchema().getColumnSchema()[keyIndex].getType();
                switch (type) {
                    case Types.SMALLINT:
                        if (obj instanceof Number) {
                            obj = ((Number) obj).shortValue();
                        } else if (obj instanceof String) {
                            obj = Short.parseShort((String) obj);
                        }
                        break;
                    case Types.INTEGER:
                        if (obj instanceof Number) {
                            obj = ((Number) obj).intValue();
                        } else if (obj instanceof String) {
                            obj = Integer.parseInt((String) obj);
                        }
                        break;
                    case Types.BIGINT:
                        if (obj instanceof Number) {
                            obj = ((Number) obj).longValue();
                        } else if (obj instanceof String) {
                            obj = Long.parseLong((String) obj);
                        }
                        break;
                    default:
                }
                ret.setObject(keyIndex, obj);
            }
        }

        return ret;
    }
}
