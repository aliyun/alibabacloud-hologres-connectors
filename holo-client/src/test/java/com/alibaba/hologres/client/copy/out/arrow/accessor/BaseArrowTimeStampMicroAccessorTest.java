package com.alibaba.hologres.client.copy.out.arrow.accessor;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * 验证 BaseArrowTimeStampMicroAccessor 将 arrow 中的 epoch 微秒转换为 Timestamp: 1970 年之前的负微秒不能向零截断,
 * 且亚毫秒精度需完整保留.
 */
public class BaseArrowTimeStampMicroAccessorTest {

    private static final long MICROS_2023 = 1700000000123456L;
    // 1969-12-31 23:59:59.999999 (UTC epoch 微秒), 即 epoch -1 微秒
    private static final long MICROS_PRE_1970_SUBSECOND = -1L;
    // 1969-12-31 00:00:00 (整天, 负值但无亚毫秒部分)
    private static final long MICROS_PRE_1970_WHOLE_DAY = -86400L * 1_000_000L;

    /**
     * 期望的 wall-clock Timestamp: epoch 毫秒减去 accessor 使用的时区偏移后按本地时区渲染, 微秒部分完整保留. 毫秒部分必须用 floorDiv(而非
     * /)计算, 这正是旧实现向零截断出错的地方.
     */
    private static Timestamp expectedTimestamp(long micros) {
        long millis = Math.floorDiv(micros, 1000L);
        long offsetMs = TimeZone.getDefault().getOffset(millis);
        LocalDateTime wall =
                Instant.ofEpochMilli(millis - offsetMs)
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
        Timestamp ts = Timestamp.valueOf(wall);
        ts.setNanos((int) (Math.floorMod(micros, 1_000_000L) * 1000));
        return ts;
    }

    @Test
    void testGetTimestamp() {
        try (BufferAllocator allocator = new RootAllocator();
                TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
            vector.allocateNew(4);
            vector.set(0, MICROS_2023);
            vector.set(1, MICROS_PRE_1970_SUBSECOND);
            vector.set(2, MICROS_PRE_1970_WHOLE_DAY);
            // index 3 保持 null
            vector.setValueCount(4);

            BaseArrowTimeStampMicroAccessor accessor = new BaseArrowTimeStampMicroAccessor(vector);

            // 现代时间: 微秒精度完整保留
            assertEquals(accessor.get(0), expectedTimestamp(MICROS_2023));

            // 1970 年之前且带亚毫秒: 负微秒不能向零截断成 epoch 0,
            // 旧实现 micros/1000 会得到 0, 且负数取模传入 setNanos 会抛 IllegalArgumentException
            assertEquals(accessor.get(1), expectedTimestamp(MICROS_PRE_1970_SUBSECOND));

            // 1970 年之前整天: 负值整除结果正确
            assertEquals(accessor.get(2), expectedTimestamp(MICROS_PRE_1970_WHOLE_DAY));

            assertNull(accessor.get(3));
        }
    }
}
