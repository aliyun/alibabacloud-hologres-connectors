## 依赖hologres-connector-flink-base，实现了Flink 1.13版本的Connector

- 支持结果表、维表、批量源表（connector-1.2版本开始支持）

## 准备工作

- 需要**Hologres 0.9**及以上版本。
- 需要Flink1.13

### 从中央仓库获取jar

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。

```xml

<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-flink-1.13</artifactId>
    <version>1.3.0</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

### 自行编译

connector依赖父项目的pom文件，在本项目根目录执行以下命令进行install

```
mvn clean install -N
```

#### build base jar 并 install 到本地maven仓库

- -P指定相关版本参数


  ```
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
| endpoint    | hologres endpoint    | 是 |  |
| username | 用户名 | 是 |  |
| password | 密码 | 是 |  |

### 连接参数

| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
| connectionSize| 单个Flink Hologres Task所创建的JDBC连接池大小。|否|默认值为3，和吞吐成正比。|
| connectionPoolName| 连接池名称，同一个TaskManager中，表配置同名的连接池名称可以共享连接池 |否|无默认值，每个表默认使用自己的连接池。如果设置连接池名称，则所有表的connectionSize需要相同|
| fixedConnectionMode| 写入和点查不占用连接数（beta功能，需要connector版本>=1.2.0，hologres引擎版本>=1.3）|   否|   默认值：false|
| jdbcRetryCount| 当连接故障时，写入和查询的重试次数|   否|   默认值：10| 
| jdbcRetrySleepInitMs | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |  否 |  默认值：1000 ms| 
| jdbcRetrySleepStepMs | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs  | 否|   默认值：5000 ms| 
| jdbcConnectionMaxIdleMs | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放|    否 |  默认值：60000 ms| 
| jdbcMetaCacheTTL | TableSchema信息的本地缓存时间   | 否 |  默认值：60000 ms| 
| jdbcMetaAutoRefreshFactor | 当TableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache |   否|   默认值：-1, 表示不自动刷新| 

### 写入结果表参数

| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
| mutatetype | 流式写入语义，见下面“流式语义”一节<br /> | 否 | 默认值：insertorignore |
| ignoredelete | 是否忽略撤回消息 | 否 | 默认值：true，只在流式语义下有效。<br> 在DataStream作业中，用户使用自定义的record类型，因此默认不支持delete消息。如果需要支持消息的回撤，需要在实现RecordConverter时对convert结果设置MutationType，详见hologres-connector-examples子项目示例。 |
| createparttable| 当写入分区表时，是否自动根据分区值自动创建分区表 | 否|默认值为false。建议慎用该功能，确保分区值不会出现脏数据导致创建错误的分区表。|
| jdbcWriteBatchSize| Hologres Sink节点数据攒批的最大批大小 | 否 |默认值为256|
| jdbcWriteBatchByteSize|    Hologres Sink节点单个线程数据攒批的最大字节大小    | 否 | 默认值：20971520（2 * 1024 * 1024），2MB|
| jdbcWriteBatchTotalByteSize|    Hologres Sink节点所有数据攒批的最大字节大小    | 否 | 默认值：20971520（20 * 1024 * 1024），20MB|
| jdbcWriteFlushInterval |Hologres Sink节点数据攒批的最长Flush等待时间）|否 |默认值为10000，即10秒|
| jdbcUseLegacyPutHandler|true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict|否 | 默认值：false | 
| jdbcEnableDefaultForNotNullColumn|设置为true时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00|    否|    默认值：true|
| jdbcCopyWriteMode | 是否使用fixed copy方式写入 | 否 | 默认值false。fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)，但不支持回撤。|
| jdbcCopyWriteFormat | 底层是否走二进制协议 | 否 | 默认为binary。表示使用二进制模式，二进制会更快，否则为文本模式。|
| jdbcCopyWriteDirectConnect | copy模式是否直连 | 否 | 默认值为true。copy的瓶颈往往是VIP endpoint的网络吞吐，因此copy写入模式下会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。|

### 维表查询参数

| 参数 | 参数说明 | 是否必填 | 备注 |
| :---: | :---: | :---: | :---: |
|jdbcReadBatchSize|    维表点查最大批次大小|    否|    默认值：128 |
|jdbcReadBatchQueueSize|维表点查请求缓冲队列大小|否|默认值：256 |
|async|是否采用异步方式同步数据|否|默认值：false。异步模式可以并发地处理多个请求和响应，从而连续的请求之间不需要阻塞等待，提高查询的吞吐。但在异步模式下，无法保证请求的绝对顺序。 |
|cache|缓存策略|否|Hologres仅支持以下两种缓存策略：<br/>None（默认值）：无缓存。<br/>LRU：缓存维表里的部分数据。源表的每条数据都会触发系统先在Cache中查找数据，如果未找到，则去物理维表中查找。 |
|cachesize|缓存大小。|否|选择LRU缓存策略后，可以设置缓存大小，默认值为10000行。 |
|cachettlms|更新缓存的时间间隔，单位为毫秒。|否|当选择LRU缓存策略后，可以设置缓存失效的超时时间，默认不过期。|
|cacheempty|是否缓存join结果为空的数据。|否|取值如下：<br/>true（默认值）：表示缓存join结果为空的数据。<br/>false：表示不缓存join结果为空的数据。 |

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
