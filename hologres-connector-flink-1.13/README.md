## 依赖hologres-connector-flink-base和hologres-connector-flink-1.12，实现了Flink 1.13版本的Connector

## 准备工作
- 需要**Hologres 0.9**及以上版本。
- 需要Flink1.13

### 从中央仓库获取jar
可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。
```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-flink-1.13</artifactId>
    <version>1.0.0</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```
### 自行编译
#### build base jar 并 install 到本地maven仓库
- -P指定相关版本参数
- 为了精简代码，Flink1.13的connector依赖了Flink1.12的connector，需要先编译安装1.12的connector至本地

  ```
  mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.12
  mvn install -pl hologres-connector-flink-1.12 clean package -DskipTests
  mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.13
  ```

#### build jar

  ```
    mvn install -pl hologres-connector-flink-1.13 clean package -DskipTests
  ```

## 使用示例

见 hologres-connector-examples子项目

## Hologres Flink Connector相关参数说明

### 必要参数
| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
| connector | connector类型 | 是 | 值为：hologres |
| dbname | 读取的数据库 | 是 |  |
| tablename | 读取的表 | 是 |  |
| endpoint	| hologres endpoint	| 是 |  |
| username | 用户名 | 是 |  |
| password | 密码 | 是 |  |

### 连接参数
| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
| connectionSize| 单个Flink Hologres Task所创建的JDBC连接池大小。|否|默认值为3，和吞吐成正比。|
| jdbcRetryCount| 当连接故障时，写入和查询的重试次数|   否|   默认值：10| 
| jdbcRetrySleepInitMs | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |  否 |  默认值：1000 ms| 
| jdbcRetrySleepStepMs | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs  | 否|   默认值：5000 ms| 
| jdbcConnectionMaxIdleMs | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放|    否 |  默认值：60000 ms| 
| jdbcMetaCacheTTL | TableSchema信息的本地缓存时间   | 否 |  默认值：60000 ms| 
| jdbcMetaAutoRefreshFactor | 当TableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache |   否|   默认值：-1, 表示不自动刷新| 

### 写入参数
| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
| mutatetype | 流式写入语义，见下面“流式语义”一节<br /> | 否 | 默认值：insertorignore |
| ignoredelete | 是否忽略撤回消息 | 否 | 默认值：false，只在流式语义下有效 |
| createparttable| 当写入分区表时，是否自动根据分区值自动创建分区表 | 否|默认值为false。建议慎用该功能，确保分区值不会出现脏数据导致创建错误的分区表。|
| jdbcWriteBatchSize| Hologres Sink节点数据攒批的最大批大小 | 否 |默认值为256|
| jdbcWriteBatchByteSize| 	Hologres Sink节点单个线程数据攒批的最大字节大小	| 否 | 默认值：20971520（2 * 1024 * 1024），2MB|
| jdbcWriteBatchTotalByteSize| 	Hologres Sink节点所有数据攒批的最大字节大小	| 否 | 默认值：20971520（20 * 1024 * 1024），20MB|
| jdbcWriteFlushInterval |Hologres Sink节点数据攒批的最长Flush等待时间）|否 |默认值为10000，即10秒|
| jdbcReWriteBatchedDeletes|将多条delete请求合并为一条sql语句提升性能|否 | 默认值：true | 
| jdbcRewriteSqlMaxBatchSize|单条sql进行INSERT/DELETE操作的最大批次大小,比如写入操作，所攒的批会通过 writeBatchSize/rewriteSqlMaxBatchSize 条INSERT语句完成插入	| 否	| 默认值：1024|
| jdbcEnableDefaultForNotNullColumn|设置为true时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00| 	否| 	默认值：true| 

### Hologres Flink Connector 流式语义

流式语义适用于持续不断往Hologres写入数据

- 流式语义做不做checkpointing，需要根据sink的配置和Hologres表的属性分为 exactly-once 或 at-least-once 语义
- 当Hologres表设有primary key时，Hologres sink可通过幂等性（idempotent）提供exactly-once语义
  - 在有primary key的情况下，出现同pk数据出现多次的情况时，Hologres sink支持以下流式语
    1. insertOrIgnore：保留首次出现的数据，忽略之后的所有数据。默认语义
    1. insertOrReplace：后出现的数据整行替换已有数据
    1. insertOrUpdate：部分替换已有数据。<br />a. 比如一张表有a,b,c,d四个字段，a是pk，然后写入的时候只写入a,b两个字段，在pk重复的情况下，会只更新b字段，c,d原有的值不变

> 说明：
> 1. hologres真实表含有主键的时候，实时写入默认会丢弃后来到来的数据来做去重(insertOrIngore)
> 2. 当mutatetype设置为insertOrUpdate或者insertOrReplace的时候会根据主键做更新
