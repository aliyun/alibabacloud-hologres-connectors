package com.alibaba.hologres.connector.flink.sink.v2.stage;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/** Tests for the asynchronous Stage INSERT retry policy. */
public class HologresStreamingStageSinkWriterTest {

    @Test
    public void testFirstAttemptSucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();

        Exception failure =
                HologresStreamingStageSinkWriter.executeWithRetries(
                        HologresStreamingStageSinkWriter.MAX_INSERT_RETRIES,
                        attempts::incrementAndGet,
                        (retryNumber, exception) -> retries.incrementAndGet());

        assertNull(failure);
        assertEquals(1, attempts.get());
        assertEquals(0, retries.get());
    }

    @Test
    public void testThirdRetrySucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();

        Exception failure =
                HologresStreamingStageSinkWriter.executeWithRetries(
                        HologresStreamingStageSinkWriter.MAX_INSERT_RETRIES,
                        () -> {
                            if (attempts.incrementAndGet() <= 3) {
                                throw new IOException("temporary failure");
                            }
                        },
                        (retryNumber, exception) -> {
                            retries.incrementAndGet();
                            assertEquals(retries.get(), retryNumber);
                        });

        assertNull(failure);
        assertEquals(4, attempts.get());
        assertEquals(3, retries.get());
    }

    @Test
    public void testRetryExhaustionReturnsFirstFailure() {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        IOException firstFailure = new IOException("first failure");

        Exception failure =
                HologresStreamingStageSinkWriter.executeWithRetries(
                        HologresStreamingStageSinkWriter.MAX_INSERT_RETRIES,
                        () -> {
                            if (attempts.incrementAndGet() == 1) {
                                throw firstFailure;
                            }
                            throw new IOException("later failure");
                        },
                        (retryNumber, exception) -> retries.incrementAndGet());

        assertSame(firstFailure, failure);
        assertEquals(4, attempts.get());
        assertEquals(3, retries.get());
    }
}
