/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Sliding window rate limiter with token refill at window boundaries.
 *
 * <p>This rate limiter allows a configurable number of requests per second (RPS). Tokens are
 * refilled to full capacity at the beginning of each window.
 */
public class RateLimiter {
    public static final Logger LOGGER = LoggerFactory.getLogger(RateLimiter.class);
    private static final long DEFAULT_WINDOW_SIZE_MS = 1000L; // 1 second

    private final long windowSizeNanos;
    private final int tokensPerWindow;
    private long windowStartNanos;
    private int availableTokens;
    private final Object lock = new Object();

    /**
     * Create a rate limiter with the specified RPS and default window size (1 second).
     *
     * @param rps requests per second (tokens per window)
     */
    public RateLimiter(int rps) {
        this(rps, DEFAULT_WINDOW_SIZE_MS);
    }

    /**
     * Create a rate limiter with the specified RPS and window size.
     *
     * @param rps requests per second (tokens per window)
     * @param windowSizeMs window size in milliseconds
     */
    public RateLimiter(int rps, long windowSizeMs) {
        if (rps <= 0) {
            throw new IllegalArgumentException("RPS must be positive");
        }
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("Window size must be positive");
        }
        this.tokensPerWindow = rps;
        this.windowSizeNanos = TimeUnit.MILLISECONDS.toNanos(windowSizeMs);
        this.windowStartNanos = System.nanoTime();
        this.availableTokens = rps; // Start with full tokens
    }

    /**
     * Acquire a single token. Blocks if no tokens available until next window.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void acquire() throws InterruptedException {
        acquire(1);
    }

    /**
     * Acquire multiple tokens. Blocks if insufficient tokens until enough accumulate.
     *
     * @param permits number of tokens to acquire
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void acquire(int permits) throws InterruptedException {
        if (permits <= 0) {
            throw new IllegalArgumentException("Permits must be positive");
        }

        synchronized (lock) {
            while (true) {
                refillIfNeeded();

                if (availableTokens >= permits) {
                    availableTokens -= permits;
                    return;
                }

                // Calculate wait time until next window
                long now = System.nanoTime();
                long elapsed = now - windowStartNanos;
                long waitNanos = windowSizeNanos - elapsed;

                if (waitNanos > 0) {
                    long waitMs = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                    int waitNanosRemainder = (int) (waitNanos % 1_000_000);
                    lock.wait(waitMs, waitNanosRemainder);
                    LOGGER.info("Waiting for {} ms due to rate limit", waitMs);
                }
            }
        }
    }

    /**
     * Try to acquire a token without blocking.
     *
     * @return true if acquired, false otherwise
     */
    public boolean tryAcquire() {
        synchronized (lock) {
            refillIfNeeded();
            if (availableTokens > 0) {
                availableTokens--;
                return true;
            }
            return false;
        }
    }

    /**
     * Try to acquire a token with timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if acquired, false if timeout elapsed
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        long timeoutNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + timeoutNanos;

        synchronized (lock) {
            while (true) {
                refillIfNeeded();

                if (availableTokens > 0) {
                    availableTokens--;
                    return true;
                }

                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }

                long waitNanos =
                        Math.min(
                                remaining,
                                windowSizeNanos - (System.nanoTime() - windowStartNanos));
                if (waitNanos > 0) {
                    long waitMs = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                    int waitNanosRemainder = (int) (waitNanos % 1_000_000);
                    lock.wait(waitMs, waitNanosRemainder);
                    LOGGER.info("Waiting for {} ms due to rate limit", waitMs);
                }
            }
        }
    }

    /**
     * Check if we've moved to a new window and refill tokens. Must be called while holding lock.
     */
    private void refillIfNeeded() {
        long now = System.nanoTime();
        long elapsed = now - windowStartNanos;

        if (elapsed >= windowSizeNanos) {
            windowStartNanos = now;
            availableTokens = tokensPerWindow; // Refill to full capacity
        }
    }

    /**
     * Get current available tokens (for monitoring/testing).
     *
     * @return the number of available tokens
     */
    public int getAvailableTokens() {
        synchronized (lock) {
            refillIfNeeded();
            return availableTokens;
        }
    }

    /**
     * Get the tokens per window (RPS).
     *
     * @return tokens per window
     */
    public int getTokensPerWindow() {
        return tokensPerWindow;
    }
}
