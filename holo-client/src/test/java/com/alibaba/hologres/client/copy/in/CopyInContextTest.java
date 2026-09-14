package com.alibaba.hologres.client.copy.in;

import com.alibaba.hologres.client.copy.CopyMode;
import com.alibaba.hologres.client.model.HoloVersion;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/** Unit tests for bulk-load binary version compatibility. */
public class CopyInContextTest {
    private static final String BINARY_VERSION_ERROR = "binary 格式仅在 Hologres 5.0.0 及以上版本可用，当前版本不支持";

    @Test
    public void testBulkLoadBinaryRequiresHologres5() {
        assertVersionRejected(new HoloVersion("4.2.9"), CopyMode.BULK_LOAD);
        assertVersionRejected(new HoloVersion("4.2.9"), CopyMode.BULK_LOAD_ON_CONFLICT);
        assertVersionRejected(null, CopyMode.BULK_LOAD);
    }

    @Test
    public void testBulkLoadBinaryAcceptsHologres5() throws Exception {
        CopyInContext.checkBulkLoadBinaryVersion(new HoloVersion("5.0.0"), CopyMode.BULK_LOAD);
        CopyInContext.checkBulkLoadBinaryVersion(
                new HoloVersion("5.1.0"), CopyMode.BULK_LOAD_ON_CONFLICT);
    }

    @Test
    public void testStreamBinaryDoesNotUseBulkLoadGate() throws Exception {
        CopyInContext.checkBulkLoadBinaryVersion(new HoloVersion("4.2.9"), CopyMode.STREAM);
    }

    private static void assertVersionRejected(HoloVersion version, CopyMode copyMode) {
        try {
            CopyInContext.checkBulkLoadBinaryVersion(version, copyMode);
            Assert.fail("Expected bulk-load binary version gate to reject " + version);
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains(BINARY_VERSION_ERROR));
        }
    }
}
