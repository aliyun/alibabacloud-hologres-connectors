package com.alibaba.hologres.connector.flink.sink.v2.stage;

import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.connector.flink.config.HologresConnectionParam;
import com.alibaba.hologres.connector.flink.config.JDBCOptions;
import com.alibaba.hologres.org.postgresql.PGProperty;
import org.junit.Test;

import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for STAGE INSERT session configuration. */
public class StageHelperTest {

    @Test
    public void testEnableQueryIdNoticeBeforeInsert() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);

        StageHelper.configureInsertSession(statement, param, -1, true);

        verify(statement).execute("set hg_experimental_display_query_id = on");
    }

    @Test
    public void testGetQueryIdFromNotice() throws Exception {
        Statement statement = mock(Statement.class);
        SQLWarning first = new SQLWarning("unrelated notice");
        first.setNextWarning(new SQLWarning("QueryID: 1002003004005"));
        when(statement.getWarnings()).thenReturn(first);

        assertEquals("1002003004005", StageHelper.getQueryIdFromNotice(statement));
    }

    @Test
    public void testStageConnectionsSetApplicationName() {
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        JDBCOptions jdbcOptions = mock(JDBCOptions.class);
        when(param.getUsername()).thenReturn("user");
        when(param.getPassword()).thenReturn("password");
        when(param.getJdbcOptions()).thenReturn(jdbcOptions);
        when(jdbcOptions.getDbUrl()).thenReturn("jdbc:hologres://host:80/db");

        Properties props = StageHelper.getConnectionProperties(param);
        assertEquals("hologres-connector-flink-stage", PGProperty.APPLICATION_NAME.get(props));

        HoloConfig holoConfig = StageHelper.createHoloConfig(param);
        assertEquals("hologres-connector-flink-stage", holoConfig.getAppName());
    }

    @Test
    public void testStageCreateRetriesBusyFailureOnce() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        StageHelper.executeStageCreateWithRetry(
                () -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new java.sql.SQLException(
                                "MemTable Usage Exceed Reject Limit", "53000");
                    }
                },
                0,
                "test_stage");

        assertEquals(2, attempts.get());
    }

    @Test
    public void testStageCreateDoesNotRetryNonRetryableFailure() {
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(
                java.sql.SQLException.class,
                () ->
                        StageHelper.executeStageCreateWithRetry(
                                () -> {
                                    attempts.incrementAndGet();
                                    throw new java.sql.SQLException("permission denied", "42501");
                                },
                                0,
                                "test_stage"));

        assertEquals(1, attempts.get());
    }

    @Test
    public void testStageCreateFailsAfterRetryIsExhausted() {
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(
                java.sql.SQLException.class,
                () ->
                        StageHelper.executeStageCreateWithRetry(
                                () -> {
                                    attempts.incrementAndGet();
                                    throw new java.sql.SQLException(
                                            "MemTable Usage Exceed Reject Limit", "53000");
                                },
                                0,
                                "test_stage"));

        assertEquals(2, attempts.get());
    }

    @Test
    public void testStageCreateRetryClassification() {
        assertTrue(
                StageHelper.isRetryableStageCreateFailure(
                        new java.sql.SQLException("resource busy", "53000")));
        assertFalse(
                StageHelper.isRetryableStageCreateFailure(
                        new java.sql.SQLException("invalid parameter", "22023")));
    }

    @Test
    public void testValidateStageNameRejectsUnexpectedCharacters() {
        StageHelper.validateStageName("flink_stage_1_2_3_4");

        assertThrows(
                IllegalArgumentException.class,
                () -> StageHelper.validateStageName("flink_stage_1';drop table t;--"));
    }

    @Test
    public void testEnableInternalStageSerialReadBeforeInsert() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);
        when(param.isEnableReshuffleByHolo()).thenReturn(false);

        StageHelper.configureInsertSession(statement, param, -1, true);

        verify(statement).execute("set hg_experimental_internal_stage_serial_read = on");
    }

    @Test
    public void testDisableInternalStageSerialReadBeforeInsert() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);
        when(param.isEnableReshuffleByHolo()).thenReturn(false);

        StageHelper.configureInsertSession(statement, param, -1, false);

        verify(statement).execute("set hg_experimental_internal_stage_serial_read = off");
    }

    @Test
    public void testStreamingStageAlwaysSetsTargetShard() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);
        when(param.isEnableReshuffleByHolo()).thenReturn(false);

        StageHelper.configureInsertSession(statement, param, 7, true);

        verify(statement).execute("set hg_experimental_target_shard_list = '7'");
    }

    @Test
    public void testStreamingStageSetsCompleteTargetShardList() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);

        StageHelper.configureInsertSession(statement, param, Arrays.asList(0, 10), true);

        verify(statement).execute("set hg_experimental_target_shard_list = '0,10'");
        verify(statement).execute("set hg_experimental_internal_stage_serial_read = on");
    }

    @Test
    public void testBatchStageDoesNotSetTargetShard() throws Exception {
        Statement statement = mock(Statement.class);
        HologresConnectionParam param = mock(HologresConnectionParam.class);
        when(param.isEnableServerlessComputing()).thenReturn(false);
        when(param.isEnableReshuffleByHolo()).thenReturn(true);

        StageHelper.configureInsertSession(statement, param, -1, false);

        verify(statement, never()).execute(startsWith("set hg_experimental_target_shard_list"));
    }

    @Test
    public void testConfigureStatementTimeoutRoundsUpToSeconds() throws Exception {
        Statement statement = mock(Statement.class);

        StageHelper.configureStatementTimeout(statement, 60_001L);

        verify(statement).setQueryTimeout(61);
    }

    @Test
    public void testConfigureStatementTimeoutDisabled() throws Exception {
        Statement statement = mock(Statement.class);

        StageHelper.configureStatementTimeout(statement, 0);

        verify(statement, never()).setQueryTimeout(org.mockito.ArgumentMatchers.anyInt());
    }
}
