package com.alibaba.hologres.shipper.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessBar {
    public class Meter {
        private long lastCount;
        private AtomicLong count;
        private AtomicInteger parallelNum;
        private long lastTimeMs;
        private ProcessBar bar;

        public Meter(ProcessBar bar) {
            lastTimeMs = System.currentTimeMillis();
            count = new AtomicLong(0L);
            lastCount = 0;
            parallelNum = new AtomicInteger(0);
            this.bar = bar;
        }

        public void mark() {
            this.mark(1L);
        }

        public void mark(long n) {
            this.count.addAndGet(n);
            this.bar.addTotalBytes(n);
        }

        public long getSpeedSinceLast() {
            long now = System.currentTimeMillis();
            long curCount = count.get();
            long rate = (curCount - lastCount)  / (now - lastTimeMs);
            this.lastTimeMs = now;
            lastCount = curCount;
            return rate;
        }

        public long getCountValue() {
            return count.get();
        }

        public void restart() {
            lastTimeMs = System.currentTimeMillis();
            count.set(0L);
            lastCount = 0;
            parallelNum.set(0);
        }

        public void incrementParallelNum() {
            parallelNum.incrementAndGet();
        }

        public void decrementParallelNum() {
            parallelNum.decrementAndGet();
        }

        public int getParallelNum() {
            return parallelNum.get();
        }
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(ProcessBar.class);
    private Integer totalCount;
    private AtomicInteger finishCount;
    private AtomicInteger successCount;
    private AtomicInteger failedCount;
    private AtomicLong totalBytes;
    private ConcurrentHashMap<String, Meter> tableToMeters;
    private long startTimeMs;
    private long lastPrintTimeMs;
    private long lastTotalBytes;

    public ProcessBar(Integer total) {
        tableToMeters = new ConcurrentHashMap<>();
        totalCount = total;
        finishCount = new AtomicInteger(0);
        successCount = new AtomicInteger(0);
        failedCount = new AtomicInteger(0);
        totalBytes = new AtomicLong(0L);
        startTimeMs = System.currentTimeMillis();
        lastPrintTimeMs = startTimeMs;
        lastTotalBytes = 0;
    }

    public void generateProcess(String dbName, String tableName) {
        Meter meter = new Meter(this);
        tableToMeters.put(String.join(".", dbName, tableName), meter);
    }

    public Meter getTableMeter(String dbName, String tableName) {
        return tableToMeters.get(String.join(".", dbName, tableName));
    }

    public void markSuccess(String dbName, String tableName) {
        tableToMeters.remove(String.join(".", dbName, tableName));
        finishCount.incrementAndGet();
        successCount.incrementAndGet();
    }

    public void markFailed(String dbName, String tableName) {
        tableToMeters.remove(String.join(".", dbName, tableName));
        finishCount.incrementAndGet();
        failedCount.incrementAndGet();
    }

    public void addTotalBytes(long n) {
        this.totalBytes.addAndGet(n);
    }

    public void print() {
        Integer finishedCount = this.finishCount.get();
        Integer succeccedCount = this.successCount.get();
        Integer failedCount = this.failedCount.get();
        Integer percent = finishedCount *100 / this.totalCount;

        StringBuilder tableSpeedStrBuilder = new StringBuilder();
        if (!tableToMeters.isEmpty()) {
            tableSpeedStrBuilder.append("*************Ship Table Speed**************\n");
            for (Map.Entry<String, Meter> entry : tableToMeters.entrySet()) {
                Meter meter = entry.getValue();
                long speed = meter.getSpeedSinceLast();
                int parallelNum = meter.getParallelNum();
                long bytesWritten = meter.getCountValue();
                tableSpeedStrBuilder.append(entry.getKey()).append(" has written ").append(bytesWritten).append(" Bytes,").append("current speed: ").append(speed).append(" KByte/s, current parallel num: ").append(parallelNum).append("\n");
            }
        }

        long now = System.currentTimeMillis();
        long curTotalBytes = this.totalBytes.get();
        long curSpeed = (curTotalBytes - lastTotalBytes)  / (now - lastPrintTimeMs);
        long avgSpeed = curTotalBytes / (now - startTimeMs);
        this.lastPrintTimeMs = now;
        lastTotalBytes = curTotalBytes;

        LOGGER.info("{}total progress:(finished:{} / total:{}) percent {}%, successed: {}, failed: {}, written {} Bytes, current speed {} KByte/s, total avg speed {} KByte/s", tableSpeedStrBuilder, finishedCount, this.totalCount, percent, succeccedCount, failedCount, curTotalBytes, curSpeed, avgSpeed);
    }

}
