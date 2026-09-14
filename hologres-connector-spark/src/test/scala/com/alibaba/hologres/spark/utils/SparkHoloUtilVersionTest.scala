package com.alibaba.hologres.spark.utils

import com.alibaba.hologres.client.model.HoloVersion
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

/** Tests for COPY OUT Arrow Hologres version compatibility. */
class SparkHoloUtilVersionTest {

  @Test
  def testCopyOutArrowSupportedVersions(): Unit = {
    assertFalse(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(3, 9, 99)))
    assertFalse(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(4, 0, 17)))
    assertTrue(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(4, 0, 18)))
    assertTrue(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(4, 0, 99)))
    assertTrue(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(4, 1, 0)))
    assertTrue(SparkHoloUtil.isCopyOutArrowSupported(new HoloVersion(5, 0, 0)))
  }
}
