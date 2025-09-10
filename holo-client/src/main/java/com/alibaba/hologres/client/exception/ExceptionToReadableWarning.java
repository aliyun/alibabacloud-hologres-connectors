/*
 * Copyright (c) 2023. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.exception;

import org.postgresql.util.PSQLException;
import org.slf4j.Logger;

import java.io.EOFException;
import java.net.SocketException;

/** 对捕获到但仅打印日志的异常进行处理,提高可读性. */
public class ExceptionToReadableWarning {

    public static void readableWarn(Logger LOG, String message, Throwable e) {
        String readable = makeReadable(e);
        if (readable != null) {
            LOG.warn(message + ". \nPossible causes [{}]", readable);
        } else {
            LOG.warn(message, e);
        }
    }

    public static String makeReadable(Throwable e) {
        if (e instanceof PSQLException) {
            PSQLException psqlException = (PSQLException) e;
            // 检查消息和原因是否符合特定条件
            String message = psqlException.getMessage();
            Throwable cause = psqlException.getCause();
            if (message != null
                    && (message.contains("Database connection failed when reading from copy")
                            || message.contains(
                                    "Database connection failed when writing to copy"))) {
                if (cause instanceof EOFException) {
                    // EOF: walsender进程超时断开
                    return message
                            + ": While consuming Binlog, the connection was closed by the server due to timeout. "
                            + "This may occur if the job experienced backpressure, causing the consumer thread to be unable to read the binlog for a long time.";
                } else if (cause instanceof SocketException) {
                    // Connection reset (by peer)
                    // Broken pipe (Write failed)
                    return message
                            + ": While consuming Binlog, the connection was unexpectedly closed by the server."
                            + "Situations that may occur include server timeouts, too many connections leading to cleanup, or node restarts.";
                }
            }
        }

        return null;
    }
}
