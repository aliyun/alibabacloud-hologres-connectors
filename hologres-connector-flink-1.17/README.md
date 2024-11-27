## 依赖hologres-connector-flink-base，实现了Flink 1.17版本的Connector

- 支持结果表、维表、批量源表（connector-1.2版本开始支持）

## 准备工作

- 需要**Hologres 0.9**及以上版本, 建议使用2.1及以上版本。
- 需要Flink1.17

### 从中央仓库获取jar

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。

```xml

<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-flink-1.17</artifactId>
    <version>1.5.0</version>
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
mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.17
```

#### build jar

```
mvn install -pl hologres-connector-flink-1.17 clean package -DskipTests
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

|                参数                 |                                                                                            参数说明                                                                                             | 是否必填 |                          备注                          |
|:---------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:----:|:----------------------------------------------------:|
|          connectionSize           |                                                                             单个Flink Hologres Task所创建的JDBC连接池大小。                                                                             |  否   |                    默认值为3，和吞吐成正比。                     |
|        connectionPoolName         |                                                                          连接池名称，同一个TaskManager中，表配置同名的连接池名称可以共享连接池                                                                           |  否   | 无默认值，每个表默认使用自己的连接池。如果设置连接池名称，则所有表的connectionSize需要相同 |
|        fixedConnectionMode        |                                                                 写入和点查不占用连接数（beta功能，需要connector版本>=1.2.0，hologres引擎版本>=1.3）                                                                  |  否   |                      默认值：false                       | 
|          jdbcRetryCount           |                                                                                      当连接故障时，写入和查询的重试次数                                                                                      |  否   |                        默认值：10                        | 
|       jdbcRetrySleepInitMs        |                                                                      每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs                                                                      |  否   |                     默认值：1000 ms                      | 
|       jdbcRetrySleepStepMs        |                                                                      每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs                                                                      |  否   |                     默认值：5000 ms                      | 
|      jdbcConnectionMaxIdleMs      |                                                                              写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放                                                                               |  否   |                     默认值：60000 ms                     | 
|         jdbcMetaCacheTTL          |                                                                                    TableSchema信息的本地缓存时间                                                                                     |  否   |                     默认值：60000 ms                     | 
|     jdbcMetaAutoRefreshFactor     |                                                          当TableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache                                                           |  否   |                   默认值：-1, 表示不自动刷新                    | 
|        connection.ssl.mode        | 参数取值如下：disable（默认值）：不启用传输加密。require：启用SSL，只对数据链路加密。verify-ca：启用SSL，加密数据链路，同时使用CA证书验证Hologres服务端的真实性。verify-full：启用SSL，加密数据链路，使用CA证书验证Hologres服务端的真实性，同时比对证书内的CN或DNS与连接时配置的Hologres连接地址是否一致。 |  否   |                 默认值：disable, 表示不启用加密                 | 
| connection.ssl.root-cert.location |                                                        当connection.ssl.mode配置为verify-ca或者verify-full时，需要同时配置CA证书的路径，需要上传到flink集群环境。                                                         |  否   |                        默认值：无                         | 
|    jdbcDirectConnect     |                                                                              是否开启直连                                                                              |  否   | 默认值为false。批量写入的瓶颈往往是VIP endpoint的网络吞吐，开启此参数会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。 |
| serverless-computing.enabled| 是否使用serverless资源, 仅对读取和bulk_load写入有效,详见[serverless computing](https://help.aliyun.com/zh/hologres/user-guide/serverless-computing)  |            否              |                                          默认值:false                                          | 
| serverless-computing-query-priority|   serverless computing执行优先级   |             否             |                                            默认值:3                                            | 
| statement-timeout-seconds| query执行的超时时间 | 否 |                                    默认值:  28800000, 单位为s                                     |  

### 写入结果表参数

|                 参数                  |                                                                               参数说明                                                                               | 是否必填 |                                                                                                                                                                                                           备注                                                                                                                                                                                                           |
|:-----------------------------------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------:|:----:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|             mutatetype              |                                                                     流式写入语义，见下面“流式语义”一节<br />                                                                     |  否   |                                                                                                                                                                                                   默认值：insertorignore                                                                                                                                                                                                   |
|            ignoredelete             |                                                                             是否忽略撤回消息                                                                             |  否   |                                                                      默认值：true，只在流式语义下有效。<br> 在DataStream作业中，用户使用自定义的record类型，因此默认不支持delete消息。如果需要支持消息的回撤，需要在实现RecordConverter时对convert结果设置MutationType，详见[hologres-connector-examples子项目](hologres-connector-examples/hologres-connector-flink-examples/README.md)FlinkDataStreamToHoloExample示例。                                                                      |
|           createparttable           |                                                                     当写入分区表时，是否自动根据分区值自动创建分区表                                                                     |  否   |                                                                                                                                                                                       默认值为false。建议慎用该功能，确保分区值不会出现脏数据导致创建错误的分区表。                                                                                                                                                                                        |
|        ignoreNullWhenUpdate         |                                                         当mutatetype='insertOrUpdate'时，是否忽略更新写入数据中的Null值。                                                         |  否   |                                                                                                                                                                                                       默认值：false。                                                                                                                                                                                                       |
|         jdbcWriteBatchSize          |                                                                    Hologres Sink节点数据攒批的最大批大小                                                                     |  否   |                                                                                                                                                                                                        默认值为256                                                                                                                                                                                                         |
|       jdbcWriteBatchByteSize        |                                                                  Hologres Sink节点单个线程数据攒批的最大字节大小                                                                  |  否   |                                                                                                                                                                                           默认值：20971520（2 * 1024 * 1024），2MB                                                                                                                                                                                            |
|     jdbcWriteBatchTotalByteSize     |                                                                   Hologres Sink节点所有数据攒批的最大字节大小                                                                   |  否   |                                                                                                                                                                                          默认值：20971520（20 * 1024 * 1024），20MB                                                                                                                                                                                           |
|       jdbcWriteFlushInterval        |                                                                 Hologres Sink节点数据攒批的最长Flush等待时间）                                                                 |  否   |                                                                                                                                                                                                     默认值为10000，即10秒                                                                                                                                                                                                     |
|       jdbcUseLegacyPutHandler       | true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict |  否   |                                                                                                                                                                                                       默认值：false                                                                                                                                                                                                        | 
|  jdbcEnableDefaultForNotNullColumn  |                    设置为true时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00                    |  否   |                                                                                                                                                                                                        默认值：true                                                                                                                                                                                                        |
|    remove-u0000-in-text.enabled     |                                                               设置为true时，会自动替换text类型中非UTF-8的u0000字符                                                                |  否   |                                                                                                                                                                                                       默认值：false                                                                                                                                                                                                        |
|        deduplication.enabled        |                                                               如果一批数据中有主键相同的数据，是否进行去重，只保留最后一条到达的数据                                                                |  否   |                                                                                                                                                                                                        默认值：true                                                                                                                                                                                                        |
|         aggressive.enabled          |                                                                            是否启用激进提交模式                                                                            |  否   |                                                                                                                                                    默认值：false。设置为true时，即使攒批没有达到预期条数，只要发现连接空闲就会强制提交，在流量较小时，可以有效减少数据延时. 对insert以及fixed_copy(jdbcCopyWriteMode=STREAM)生效                                                                                                                                                     |
|        check-and-put.column         |                                                                       启用条件更新能力, 并指定检查的字段名                                                                        |  否   |                                                                                                                                                                                             默认值：无。 必须设置为holo表存在的字段名，表必须有主键                                                                                                                                                                                             |
|       check-and-put.operator        |                                                                           条件更新操作的比较操作符                                                                           |  否   |                                                                                                                                     默认值：GREATER, 表示仅当新到record的check字段大于表中原有值时，才会进行更新。目前支持配置为GREATER, GREATER_OR_EQUAL, EQUAL, NOT_EQUAL, LESS, LESS_OR_EQUAL, IS_NULL, IS_NOT_NULL                                                                                                                                     |
|        check-and-put.null-as        |                                                                当条件更新的旧数据为null时，我们把null值视为此参数配置的值                                                                 |  否   |                                                                                                                                                            默认值：无。由于postgres中，任何值与null比较，结果都是false，因此当表中原有数据为null时，想要更新，需要为其设置一个nullas参数，相当于sql中的coalesce函数                                                                                                                                                             |
|         jdbcCopyWriteMode         |                                                                           是否使用copy方式写入                                                                           |                                         否                                          | 1.4.2.1 版本之前,此参数是bool类型, 默认值false。设置为true时，默认使用fixed_copy <br> 1.4.3版本之后,此参数为enum类型, 可以取值如下  <br>STREAM: 即fixed copy,是hologres1.3新增的能力，相比insert方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)，但不支持回撤, 也不支持写入分区父表。 <br>BULK_LOAD: 批量copy,相比fixed copy，写入时使用的hologres资源更小，目前仅适用于无主键表或者主键保证不重复的有主键表（主键重复会抛出异常） <br>BULK_LOAD_ON_CONFLICT: bulkload写入有主键表时支持处理主键重复的情况,目前要求写入hologres结果表的全部字段.hologres版本2.2.27起支持 |
|        jdbcCopyWriteFormat        |                                                                            底层是否走二进制协议                                                                            |  否   |                                                                                                                                                                                          默认为binary。表示使用二进制模式，二进制会更快，否则为文本模式。                                                                                                                                                                                           |
|             bulkLoad              |                                                                          是否使用批量copy方式写入                                                                          |  否   |                                                                                                                                                 默认为false。1.4.2.1 版本之前与jdbcCopyWriteMode参数同时设置为true时生效,表示启用批量COPY, <br>1.4.3版本之后取消此参数,直接设置jdbcCopyWriteMod为BULK_LOAD即可                                                                                                                                                  |
|    reshuffle-by-holo-distribution-key.enabled     |                                                                     是否为copy连接配置目标shard list                                                                      |  否   |                                                                                 默认值为false。bulkload写入有主键表时,默认是表锁。在上游数据根据shard进行了repartition的基础上,可以开启此参数,将写入有主键表的锁粒度从表级别调整为shard级别,提高写入性能。详见[hologres-connector-examples子项目](hologres-connector-examples/hologres-connector-flink-examples/README.md)FlinkSQLToHoloRePartitionExample示例。                                                                                 |
|    sink.parallelism     |                                                                           设置sink节点的并行度                                                                           |                                         否                                          |                                                                                                                                                            默认值为false。 调整sink节点的并行度,仅在reshuffle-by-holo-distribution-key.enabled参数开启时生效,建议调整为holo结果表的shard数                                                                                                                                                             |
|       hold-on-update-before.enabled        |                                              收到上游的update_before类型的Rowdata时,是否暂时持有,直到相应update_after类型的Rowdata到来一起写入                                               |  否   |                                                                                  默认值为false。可能存在如下情况: 上游update_before的RowData先到,正好攒批满足或者FlushInterval满足写入holo,而相应的update_after字段会在下一批数据中等待写入,流量较小的情况下,update_after可能在jdbcWriteFlushInterval时间(默认10s,详见上方参数)之才写入,用户这期间在holo中无法查询到此行数据。为了尽量减少此类情况, 可以开启此参数, connector会尽量将相同主键的UB和UA一批写入                                                                                   |

### 维表查询参数

| 参数 | 参数说明 | 是否必填 |                                                  备注                                                  |
| :---: | :---: | :---: |:----------------------------------------------------------------------------------------------------:|
|jdbcReadBatchSize|    维表点查最大批次大小|    否|                                               默认值：128                                                |
|jdbcReadBatchQueueSize|维表点查请求缓冲队列大小|否|                                               默认值：256                                                |
|async|是否采用异步方式同步数据|否|              默认值：false。异步模式可以并发地处理多个请求和响应，从而连续的请求之间不需要阻塞等待，提高查询的吞吐。但在异步模式下，无法保证请求的绝对顺序。              |
|cache|缓存策略|否|Hologres仅支持以下两种缓存策略：<br/>None（默认值）：无缓存。<br/>LRU：缓存维表里的部分数据。源表的每条数据都会触发系统先在Cache中查找数据，如果未找到，则去物理维表中查找。 |
|cachesize|缓存大小。|否|                                   选择LRU缓存策略后，可以设置缓存大小，默认值为10000行。                                    |
|cachettlms|更新缓存的时间间隔，单位为毫秒。|否|                                   当选择LRU缓存策略后，可以设置缓存失效的超时时间，默认不过期。                                   |
|cacheempty|是否缓存join结果为空的数据。|否|                   取值如下：<br/>true（默认值）：表示缓存join结果为空的数据。<br/>false：表示不缓存join结果为空的数据。                   |

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
