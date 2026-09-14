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

/**
 * 测试用: 推断 AKv4 签名所需的 region.
 *
 * region 参与签名的密钥派生(aliyun_v4&lt;SK&gt; -> date -> region -> hologres -> request),
 * 服务端用自己所在 region 独立算一遍, 两边不一致就报 password authentication failed.
 * 因此 region 必须与实例真实 region 完全相符, 猜错和不填一样是失败.
 *
 * 这里没有复用 holo-client 的 AKv4AuthenticationPlugin.extractRegionFromHost, 因为它的正则
 * {{{^[^-]+-[^-]+-[^-]+-(.+?)(?:-internal|-vpc|-vpc-st)?\.hologres\.aliyuncs\.com$}}}
 * 假设 region 前面正好有 3 段连字符分隔的前缀. 对 holohub-catalog-cn-beijing 这种前缀只有
 * 2 段的 endpoint, "cn" 会被当成第 3 段前缀吃掉, 只抽出 "beijing", 签名随之失效.
 * 改为按阿里云 region id 的形态在末尾锚定匹配, 与前缀有几段无关.
 */
object AkV4RegionResolver {
  private val HoloSuffix = ".hologres.aliyuncs.com"

  /** 需要在匹配 region 之前剥掉的网络类型后缀, 长的放前面避免 -vpc 抢先匹配 -vpc-st. */
  private val NetworkSuffixes = Seq("-internal", "-vpc-st", "-vpc")

  /** cn-beijing / cn-hongkong / ap-southeast-1 / cn-shanghai-finance-1 / eu-central-1 ... */
  private val RegionShape = """(?:cn|ap|us|eu|me|na)-[a-z]+(?:-[a-z]+)*(?:-\d+)?""".r

  private val HostInJdbcUrl = """^jdbc:[^:]+://([^:/]+)""".r

  /**
   * 非公网 endpoint(裸 IP 的内网/本地部署)使用的 region. 已对内网测试实例实测确认:
   * region="local" 认证通过, 而 cn-beijing/cn-hangzhou/beijing 等均失败.
   * 这样本地也能真正跑到 AKv4 场景, 而不是整段跳过.
   */
  val LocalRegion = "local"

  /**
   * 从 host 抽取 region; 非 Hologres 公网域名(如裸 IP)或形态不认识时返回 None.
   *
   * 做法是从右往左取后缀, 找最短的合法 region. 不能用"跳过固定段数前缀"(holo-client 的做法,
   * 前缀段数不为 3 时会抽错), 也不能直接用贪婪正则在末尾匹配 —— 对
   * hgprecn-cn-test-cn-shanghai-finance-1 会一路吞到 cn-test-cn-shanghai-finance-1.
   */
  def extractRegionFromHost(host: String): Option[String] = {
    if (host == null || !host.endsWith(HoloSuffix)) {
      None
    } else {
      val withoutSuffix = host.dropRight(HoloSuffix.length)
      val prefix = NetworkSuffixes
        .find(withoutSuffix.endsWith)
        .map(s => withoutSuffix.dropRight(s.length))
        .getOrElse(withoutSuffix)
      val segments = prefix.split("-")
      (2 to segments.length).view
        .map(n => segments.takeRight(n).mkString("-"))
        .find(candidate => RegionShape.pattern.matcher(candidate).matches())
    }
  }

  /**
   * 解析顺序: 显式配置 > 公网 endpoint 按 host 推断 > 非公网 endpoint 用 [[LocalRegion]].
   *
   * 只有一种情况返回 None: host 是公网 Hologres 域名但尾部不是可识别的 region 形态
   * (例如尚未覆盖的政务云 region). 此时无从推断, 调用方应跳过 AKv4 场景,
   * 或用 HOLO_AKV4_REGION 显式指定.
   */
  def resolve(jdbcUrl: String, explicitRegion: Option[String]): Option[String] = {
    explicitRegion.map(_.trim).filter(_.nonEmpty).orElse {
      val host = HostInJdbcUrl
        .findFirstMatchIn(if (jdbcUrl == null) "" else jdbcUrl)
        .map(_.group(1))
      host match {
        case Some(h) if h.endsWith(HoloSuffix) => extractRegionFromHost(h)
        case Some(_) => Some(LocalRegion)
        case None => None
      }
    }
  }
}
