package com.alibaba.hologres.spark3

/**
 * 向后兼容别名: 保留历史路径 com.alibaba.hologres.spark3.HoloTableCatalog.
 * 新代码请使用中性路径 com.alibaba.hologres.spark.HoloTableCatalog.
 * Spark 通过反射按无参构造实例化 Catalog, 该别名子类行为完全继承中性实现,
 * 因此已有的 spark.sql.catalog.*=com.alibaba.hologres.spark3.HoloTableCatalog 配置无需修改.
 */
@deprecated("use com.alibaba.hologres.spark.HoloTableCatalog", "1.6.3")
class HoloTableCatalog extends com.alibaba.hologres.spark.HoloTableCatalog
