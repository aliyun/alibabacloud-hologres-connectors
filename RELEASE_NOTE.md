# Release Note

hologres-connectors 各版本的功能更新与缺陷修复记录。

- [1.6.1](#161)
- [1.6.0](#160)
- [1.5.6](#156)
- [1.5.4](#154)
- [1.5.3](#153)
- [1.5.1](#151)
- [1.5.0.1](#1501)
- [1.5.0](#150)
- [1.4.0](#140)
- [1.3.0](#130)

---

## 1.6.1

| 模块 | 新功能                                                                                                                                     | 缺陷修复 |
| --- |-----------------------------------------------------------------------------------------------------------------------------------------| --- |
| Spark connector | • 支持通过 Stage写入 <br>• 支持写入限流（rate limit）<br>• 支持设置分区读取方式（shard、分区、字段范围），目标可以是内表、外表或 view<br>• Arrow 方式读取支持 jsonb 类型<br>• 测试支持 jacoco 覆盖率 | |

## 1.6.0

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Flink connector | • Flink 1.19：基于开源 flink-connector-base 实现，不再依赖 vvr-common<br>• Datastream 支持写入多表<br>• Datastream 写多表支持使用 fixedcopy 模式<br>• Datastream 写多表支持忽略不存在的字段<br>• Checkpoint 时保证强制 flush<br>• 支持 time 类型 | |
| Spark connector | • 支持 ignore_null_when_update<br>• Select 读取限制 Serverless 使用的 core 数 | • 修复写入 1970 年之前的 date 类型报错的问题<br>• Overwrite 执行 DDL 时 force sync replay |
| 通用 | • 对 ignore null when update 支持走表达式方式 | |

## 1.5.6

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Spark connector | | • 修复 overwrite 过程中临时表未 drop force 的问题 |
| 通用 | • 支持 AKv4 认证 | |

## 1.5.4

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Flink connector | • 设置 read retry count，维表/insert overwrite 等场景支持重试<br>• Replace u0000 对写入 json/jsonb 的字符串也生效<br>• 脏数据异常根据 HoloClient ERRORCODE 对数据类型和数据 value 异常进行 skip | |
| Spark connector | • Select 读取限制 Serverless 使用的 core 数 | • 修复通过 Catalog 写入时未按字段顺序检查列类型的问题 |

## 1.5.3

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Spark connector | • 支持低精度数据写入高精度类型<br>• 日志包含 appname、taskid 等前缀<br>• 新增选项可禁用 right join in copy | • 修复 reshuffle 未传递 serverless 参数的问题<br>• 创建临时表之后执行 analyze |
| 其他 | • 新增 holo-llm-deepseek 模块 | |

## 1.5.1

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Flink connector | • 支持 reshuffle in SQL | • 修复一对多查询未抛出异常的问题<br>• 修复删除脏数据策略时误删 snapshot 中保存的 exception |
| Spark connector | • Catalog 重构：对应 Holo 的一个 DB，namespace 对应 schema | • 修复 select 时字段未加转义导致大写字母被转小写的问题 |

## 1.5.0.1

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| 通用 | | • 限制 Serverless 使用的 core 数，Serverless 开启时走 fixedcopy 报错问题修复<br>• 调整 copy 依赖 |

## 1.5.0

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Spark connector | • 支持通过 Hologres Table Catalog 读写<br>• 支持 pushdown predicate 和 limit 下推<br>• 支持 read from query<br>• 支持 arrow 格式读取<br>• 支持 removeU0000<br>• 提供 RepartitionUtil 用于数据分片 | • 修复 repartition 获取目标 shards 的问题 |

## 1.4.0

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Flink connector | • 支持 bulkload 写入模式和 sslmode<br>• 支持 hold on update_before 消息，减少数据不可见时间<br>• SQL task 支持按 distribution key re-shuffle<br>• 支持 CheckAndPut | |
| Spark connector | • 使用新的 copy 接口，支持 on conflict 的 bulkload<br>• Overwrite 优化：支持带 schema 的表名、特殊字符表名、分区子表 | |
| 通用 | • Flink/Spark/Hive connector 支持 Serverless computing<br>• Stream copy 支持 aggressive write<br>• 透出 bulk_load_on_conflict 参数<br>• 新增 Flink/Spark 集成测试<br>• 新增 holo-utils: find-incompatible-flink-jobs 工具 | |

## 1.3.0

| 模块 | 新功能 | 缺陷修复 |
| --- | --- | --- |
| Flink connector | • 支持 1.15 和 1.17 版本<br>• 兼容 TINYINT 类型<br>• Datastream sink 支持 delete record | • 一对多维表 join 不再排序 |
| Spark connector | • 支持 bulkload 写入及 insert overwrite<br>• 支持读取 Hologres 表 | |
| Hive connector | • Copy 模式支持设置 buffer size 和最大连接数<br>• 支持写入数组类型<br>• 支持 split read | • 正确写入 bytea 类型 |
| 其他 | • 新增 holo-chatbot、holo-llm 及 holo-e2e-performance-tool 模块 | |
