package com.alibaba.hologres.connector.flink.config;

import org.apache.flink.configuration.Configuration;

import com.alibaba.hologres.client.model.HoloVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for STAGE configuration resolution. */
public class HologresConnectionParamStageTest {

    @Test
    public void testConnectorIdentifierRemainsCompatible() {
        org.junit.Assert.assertEquals("hologres", HologresConfigs.HOLOGRES_TABLE_TYPE);
    }

    @Test
    public void testInternalStageSerialReadDefaultsByRuntimeMode() {
        assertTrue(HologresConnectionParam.resolveStageInternalStageSerialReadEnabled(null, false));
        assertFalse(HologresConnectionParam.resolveStageInternalStageSerialReadEnabled(null, true));
    }

    @Test
    public void testInternalStageSerialReadExplicitValueOverridesDefault() {
        assertFalse(
                HologresConnectionParam.resolveStageInternalStageSerialReadEnabled(false, false));
        assertTrue(HologresConnectionParam.resolveStageInternalStageSerialReadEnabled(true, true));
    }

    @Test
    public void testStageCompressionDefaultsByHologresVersion() {
        assertFalse(
                HologresConnectionParam.resolveStageCompressionEnabled(
                        null, new HoloVersion(4, 2, 7)));
        assertTrue(
                HologresConnectionParam.resolveStageCompressionEnabled(
                        null, new HoloVersion(4, 2, 8)));
        assertTrue(
                HologresConnectionParam.resolveStageCompressionEnabled(
                        null, new HoloVersion(4, 3, 0)));
    }

    @Test
    public void testStageCompressionExplicitValueOverridesVersionDefault() {
        assertTrue(
                HologresConnectionParam.resolveStageCompressionEnabled(
                        true, new HoloVersion(4, 2, 7)));
        assertFalse(
                HologresConnectionParam.resolveStageCompressionEnabled(
                        false, new HoloVersion(4, 2, 8)));
    }

    @Test
    public void testStageFlushTimeoutDefaultAndOverride() {
        Configuration configuration = new Configuration();
        assertEquals(
                300_000L, configuration.get(HologresConfigs.STAGE_FLUSH_TIMEOUT_MS).longValue());

        configuration.set(HologresConfigs.STAGE_FLUSH_TIMEOUT_MS, 123_000L);
        assertEquals(
                123_000L, configuration.get(HologresConfigs.STAGE_FLUSH_TIMEOUT_MS).longValue());

        assertEquals(2, configuration.get(HologresConfigs.STAGE_MAX_PENDING_INSERTS).intValue());
        configuration.set(HologresConfigs.STAGE_MAX_PENDING_INSERTS, 1);
        assertEquals(1, configuration.get(HologresConfigs.STAGE_MAX_PENDING_INSERTS).intValue());

        assertFalse(configuration.get(HologresConfigs.STAGE_CHECKPOINT_ASYNC_COMMIT_ENABLED));
        configuration.set(HologresConfigs.STAGE_CHECKPOINT_ASYNC_COMMIT_ENABLED, true);
        assertTrue(configuration.get(HologresConfigs.STAGE_CHECKPOINT_ASYNC_COMMIT_ENABLED));
    }
}
