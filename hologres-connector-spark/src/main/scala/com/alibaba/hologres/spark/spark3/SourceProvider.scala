package com.alibaba.hologres.spark3

/**
 * 向后兼容别名: 保留历史路径 com.alibaba.hologres.spark3.SourceProvider.
 * 新代码请使用中性路径 com.alibaba.hologres.spark.SourceProvider.
 * shortName 仍为 hologres, 通过 format("hologres") 的使用方式不受影响.
 */
@deprecated("use com.alibaba.hologres.spark.SourceProvider", "1.6.3")
class SourceProvider extends com.alibaba.hologres.spark.SourceProvider

/**
 * 兼容别名伴生对象: 重新暴露 BaseSourceProvider 中的参数常量,
 * 使旧包路径下的 SourceProvider.XXX 引用继续可用.
 */
object SourceProvider extends com.alibaba.hologres.spark.BaseSourceProvider
