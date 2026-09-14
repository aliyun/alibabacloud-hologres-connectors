/*
 *  Copyright (c) 2021, Alibaba Group;
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.hologres.spark

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AkV4RegionResolverUnitTest extends AnyFunSuite {

  test("region is extracted regardless of how many segments precede it") {
    // CI 用的 endpoint, 前缀只有 2 段. holo-client 的正则会把 cn 当前缀吃掉只剩 beijing
    assert(AkV4RegionResolver.extractRegionFromHost(
      "holohub-catalog-cn-beijing.hologres.aliyuncs.com") == Some("cn-beijing"))
    // 常见形态, 前缀 3 段
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgpostcn-cn-abc123-cn-beijing.hologres.aliyuncs.com") == Some("cn-beijing"))
  }

  test("network type suffixes are stripped before matching") {
    Seq("-internal", "-vpc", "-vpc-st").foreach { suffix =>
      assert(AkV4RegionResolver.extractRegionFromHost(
        s"hgpostcn-cn-abc123-cn-beijing$suffix.hologres.aliyuncs.com") == Some("cn-beijing"),
        s"suffix $suffix")
    }
  }

  test("multi segment regions are not over matched") {
    // 贪婪匹配会一路吞成 cn-test-cn-shanghai-finance-1, 必须取最短的合法后缀
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgprecn-cn-test-cn-shanghai-finance-1.hologres.aliyuncs.com")
      == Some("cn-shanghai-finance-1"))
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgprecn-cn-test-cn-shanghai-finance-1-vpc.hologres.aliyuncs.com")
      == Some("cn-shanghai-finance-1"))
  }

  test("regions with numeric zone and non-cn prefixes are supported") {
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgprecn-cn-xyz-ap-southeast-1.hologres.aliyuncs.com") == Some("ap-southeast-1"))
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgpostcn-cn-abc-us-west-1.hologres.aliyuncs.com") == Some("us-west-1"))
    assert(AkV4RegionResolver.extractRegionFromHost(
      "hgpostcn-cn-abc-cn-hongkong.hologres.aliyuncs.com") == Some("cn-hongkong"))
  }

  test("hosts that are not hologres public endpoints yield None") {
    assert(AkV4RegionResolver.extractRegionFromHost("11.164.92.70").isEmpty)
    assert(AkV4RegionResolver.extractRegionFromHost("localhost").isEmpty)
    assert(AkV4RegionResolver.extractRegionFromHost(null).isEmpty)
    // 域名对但尾部不是可识别的 region 形态
    assert(AkV4RegionResolver.extractRegionFromHost(
      "some-instance-name.hologres.aliyuncs.com").isEmpty)
  }

  test("both jdbc schemes used by the connector are supported") {
    // CI 实际使用的形态: jdbc:hologres:// 带端口和 database
    assert(AkV4RegionResolver.resolve(
      "jdbc:hologres://holohub-catalog-cn-beijing.hologres.aliyuncs.com:80/connector_test", None)
      == Some("cn-beijing"))
    assert(AkV4RegionResolver.resolve(
      "jdbc:postgresql://holohub-catalog-cn-beijing.hologres.aliyuncs.com:80/connector_test", None)
      == Some("cn-beijing"))
    // 不带端口
    assert(AkV4RegionResolver.resolve(
      "jdbc:hologres://holohub-catalog-cn-beijing.hologres.aliyuncs.com/connector_test", None)
      == Some("cn-beijing"))
    // jdbc:hologres 的裸 IP 形态同样落到 local
    assert(AkV4RegionResolver.resolve("jdbc:hologres://11.164.92.70:52817/postgres", None)
      == Some(AkV4RegionResolver.LocalRegion))
  }

  test("explicit region wins over host inference") {
    val url = "jdbc:postgresql://holohub-catalog-cn-beijing.hologres.aliyuncs.com:80/db"
    assert(AkV4RegionResolver.resolve(url, Some("cn-hangzhou")) == Some("cn-hangzhou"))
    // 空或空白的显式值视为未提供, 回落到推断
    assert(AkV4RegionResolver.resolve(url, Some("  ")) == Some("cn-beijing"))
    assert(AkV4RegionResolver.resolve(url, None) == Some("cn-beijing"))
  }

  test("non public endpoints fall back to the local region") {
    // 裸 IP 的内网实例: 实测 region=local 可通过认证, 因此不跳过而是用 local
    assert(AkV4RegionResolver.resolve("jdbc:postgresql://11.164.92.70:52817/postgres", None)
      == Some(AkV4RegionResolver.LocalRegion))
    assert(AkV4RegionResolver.resolve("jdbc:postgresql://localhost:5432/db", None)
      == Some(AkV4RegionResolver.LocalRegion))
    // 显式值仍然优先
    assert(AkV4RegionResolver.resolve(
      "jdbc:postgresql://11.164.92.70:52817/postgres", Some("cn-beijing")) == Some("cn-beijing"))
  }

  test("public endpoint with unrecognized region shape yields None") {
    // 公网域名但尾部不是可识别的 region 形态: 无从推断, 也不能当成 local
    assert(AkV4RegionResolver.resolve(
      "jdbc:postgresql://some-instance-name.hologres.aliyuncs.com:80/db", None).isEmpty)
  }

  test("resolve handles malformed urls") {
    assert(AkV4RegionResolver.resolve("not a jdbc url", None).isEmpty)
    assert(AkV4RegionResolver.resolve(null, None).isEmpty)
  }
}
