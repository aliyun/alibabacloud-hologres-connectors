/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.hologres.client.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** RateLimiter Tester. */
public class RateLimiterTest {

    @Test
    public void testConstructorWithValidRps() {
        RateLimiter limiter = new RateLimiter(100);
        Assert.assertEquals(limiter.getTokensPerWindow(), 100);
        Assert.assertEquals(limiter.getAvailableTokens(), 100);
    }

    @Test
    public void testConstructorWithCustomWindowSize() {
        RateLimiter limiter = new RateLimiter(50, 500);
        Assert.assertEquals(limiter.getTokensPerWindow(), 50);
        Assert.assertEquals(limiter.getAvailableTokens(), 50);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConstructorWithZeroRps() {
        new RateLimiter(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConstructorWithNegativeRps() {
        new RateLimiter(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConstructorWithZeroWindowSize() {
        new RateLimiter(100, 0);
    }

    @Test
    public void testAcquireSingleToken() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10, 100000);
        Assert.assertEquals(limiter.getAvailableTokens(), 10);

        limiter.acquire();
        Assert.assertEquals(limiter.getAvailableTokens(), 9);

        limiter.acquire();
        Assert.assertEquals(limiter.getAvailableTokens(), 8);
    }

    @Test
    public void testAcquireMultipleTokens() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10, 100000);

        limiter.acquire(3);
        Assert.assertEquals(limiter.getAvailableTokens(), 7);

        limiter.acquire(5);
        Assert.assertEquals(limiter.getAvailableTokens(), 2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAcquireZeroPermits() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(10);
        limiter.acquire(0);
    }

    @Test
    public void testTryAcquireSuccess() {
        RateLimiter limiter = new RateLimiter(10, 100000);

        Assert.assertTrue(limiter.tryAcquire());
        Assert.assertEquals(limiter.getAvailableTokens(), 9);
    }

    @Test
    public void testTryAcquireExhausted() {
        RateLimiter limiter = new RateLimiter(2, 100000);

        Assert.assertTrue(limiter.tryAcquire());
        Assert.assertTrue(limiter.tryAcquire());
        Assert.assertFalse(limiter.tryAcquire());
    }

    @Test
    public void testTryAcquireWithTimeout() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(2, 100000);

        // Exhaust tokens
        Assert.assertTrue(limiter.tryAcquire(100, TimeUnit.MILLISECONDS));
        Assert.assertTrue(limiter.tryAcquire(100, TimeUnit.MILLISECONDS));

        // Should timeout since no tokens available and window hasn't passed
        long start = System.currentTimeMillis();
        Assert.assertFalse(limiter.tryAcquire(50, TimeUnit.MILLISECONDS));
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue(elapsed >= 40, "Should have waited at least ~50ms");
    }

    @Test
    public void testTokenRefillAfterWindow() throws InterruptedException {
        // Use a short window for testing
        RateLimiter limiter = new RateLimiter(5, 100); // 100ms window

        // Exhaust all tokens
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(limiter.tryAcquire());
        }
        Assert.assertFalse(limiter.tryAcquire());

        // Wait for window to pass
        Thread.sleep(200);

        // Tokens should be refilled
        Assert.assertEquals(limiter.getAvailableTokens(), 5);
        Assert.assertTrue(limiter.tryAcquire());
    }

    @Test
    public void testBlockingAcquireWaitsForRefill() throws InterruptedException {
        // Use a short window for testing
        RateLimiter limiter = new RateLimiter(2, 5000);

        // Exhaust all tokens
        limiter.acquire(2);
        Assert.assertEquals(limiter.getAvailableTokens(), 0);

        // Acquire should block until window refills
        long start = System.currentTimeMillis();
        limiter.acquire();
        long elapsed = System.currentTimeMillis() - start;

        Assert.assertTrue(elapsed >= 4500, "Should have waited for window refill (~5000ms)");
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        final int rps = 100;
        final int numThreads = 10;
        final int requestsPerThread = 20;
        final RateLimiter limiter = new RateLimiter(rps, 3000);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numThreads);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int j = 0; j < requestsPerThread; j++) {
                                        if (limiter.tryAcquire()) {
                                            successCount.incrementAndGet();
                                        }
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    doneLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);

        // Within a single window, at most rps tokens should be acquired
        Assert.assertTrue(
                successCount.get() <= rps, "Should not acquire more than RPS tokens in one window");
        Assert.assertTrue(successCount.get() > 0, "Should acquire some tokens");
    }

    @Test
    public void testRateLimitingThroughput() throws InterruptedException {
        final int tokensPerWindow = 50;
        final long windowSizeMs = 100;
        final RateLimiter limiter = new RateLimiter(tokensPerWindow, windowSizeMs);

        final AtomicLong acquiredCount = new AtomicLong(0);
        final long testDurationMs = 500;

        Thread acquirer =
                new Thread(
                        () -> {
                            long endTime = System.currentTimeMillis() + testDurationMs;
                            while (System.currentTimeMillis() < endTime) {
                                try {
                                    limiter.acquire();
                                    acquiredCount.incrementAndGet();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        });

        acquirer.start();
        acquirer.join(testDurationMs + 200);

        // Expected: ~5 windows * 50 tokens per window = ~250 tokens
        // Allow some variance for timing
        long count = acquiredCount.get();
        Assert.assertTrue(
                count >= 200 && count <= 300,
                "Acquired count should be around 250, but was " + count);
    }
}
