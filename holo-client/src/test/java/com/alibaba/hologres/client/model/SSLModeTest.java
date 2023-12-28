package com.alibaba.hologres.client.model;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * SSLMode Tester.
 */
public class SSLModeTest {
	@Test
	public void testInitSSLModeFromString() {
		Assert.assertEquals(SSLMode.fromPgPropertyValue("disable"), SSLMode.DISABLE);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("require"), SSLMode.REQUIRE);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("verify-ca"), SSLMode.VERIFY_CA);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("verify-full"), SSLMode.VERIFY_FULL);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("DISABLE"), SSLMode.DISABLE);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("REQUIRE"), SSLMode.REQUIRE);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("VERIFY-CA"), SSLMode.VERIFY_CA);
		Assert.assertEquals(SSLMode.fromPgPropertyValue("VERIFY-FULL"), SSLMode.VERIFY_FULL);

		try {
			SSLMode.fromPgPropertyValue("unknown");
			Assert.fail();
		} catch (IllegalArgumentException e) {
			Assert.assertEquals(e.getMessage(), "invalid ssl mode: unknown");
		}
	}
}
