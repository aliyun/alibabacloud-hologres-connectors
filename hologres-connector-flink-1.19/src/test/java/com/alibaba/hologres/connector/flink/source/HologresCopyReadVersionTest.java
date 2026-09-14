package com.alibaba.hologres.connector.flink.source;

import org.apache.flink.configuration.Configuration;

import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.connector.flink.config.HologresConfigs;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.utils.HologresUtils;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for COPY OUT Arrow Hologres version compatibility. */
public class HologresCopyReadVersionTest {

    @Test
    public void testCopyOutArrowSupportedVersions() {
        assertFalse(HologresUtils.isCopyOutArrowSupported(new HoloVersion(3, 9, 99)));
        assertFalse(HologresUtils.isCopyOutArrowSupported(new HoloVersion(4, 0, 17)));
        assertTrue(HologresUtils.isCopyOutArrowSupported(new HoloVersion(4, 0, 18)));
        assertTrue(HologresUtils.isCopyOutArrowSupported(new HoloVersion(4, 0, 99)));
        assertTrue(HologresUtils.isCopyOutArrowSupported(new HoloVersion(4, 1, 0)));
        assertTrue(HologresUtils.isCopyOutArrowSupported(new HoloVersion(5, 0, 0)));
    }

    @Test
    public void testBulkReadDefaults() {
        Configuration configuration = new Configuration();
        assertEquals(
                52_428_800,
                configuration.get(HologresConfigs.SCAN_BULK_READ_MAX_BUFFER_SIZE).intValue());
        assertTrue(configuration.get(HologresConfigs.SCAN_BULK_READ_COMPRESSION_ENABLED));
    }

    @Test
    public void testConnectionParamDoesNotCaptureNonSerializableHoloVersion() {
        for (Field field : HologresConnectionParam.class.getDeclaredFields()) {
            assertFalse(
                    "HologresConnectionParam must not capture HoloVersion in the Flink job graph",
                    HoloVersion.class.isAssignableFrom(field.getType()));
        }
    }
}
