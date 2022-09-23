# 通过holo-client读写Hologres

- [通过holo-client读写Hologres](#%E9%80%9A%E8%BF%87holo-client%E8%AF%BB%E5%86%99hologres)
  - [功能介绍](#%E5%8A%9F%E8%83%BD%E4%BB%8B%E7%BB%8D)
  - [holo-client引入](#holo-client%E5%BC%95%E5%85%A5)
  - [连接数说明](#%E8%BF%9E%E6%8E%A5%E6%95%B0%E8%AF%B4%E6%98%8E)
  - [数据写入](#%E6%95%B0%E6%8D%AE%E5%86%99%E5%85%A5)
    - [写入普通表](#%E5%86%99%E5%85%A5%E6%99%AE%E9%80%9A%E8%A1%A8)
    - [写入分区表](#%E5%86%99%E5%85%A5%E5%88%86%E5%8C%BA%E8%A1%A8)
    - [写入含主键表](#%E5%86%99%E5%85%A5%E5%90%AB%E4%B8%BB%E9%94%AE%E8%A1%A8)
    - [基于主键删除（DELETE占比提高会降低整体的每秒写入）](#%E5%9F%BA%E4%BA%8E%E4%B8%BB%E9%94%AE%E5%88%A0%E9%99%A4delete%E5%8D%A0%E6%AF%94%E6%8F%90%E9%AB%98%E4%BC%9A%E9%99%8D%E4%BD%8E%E6%95%B4%E4%BD%93%E7%9A%84%E6%AF%8F%E7%A7%92%E5%86%99%E5%85%A5)
  - [数据查询](#%E6%95%B0%E6%8D%AE%E6%9F%A5%E8%AF%A2)
    - [基于完整主键查询](#%E5%9F%BA%E4%BA%8E%E5%AE%8C%E6%95%B4%E4%B8%BB%E9%94%AE%E6%9F%A5%E8%AF%A2)
    - [Scan查询](#scan%E6%9F%A5%E8%AF%A2)
  - [消费Binlog](#%E6%B6%88%E8%B4%B9binlog)
  - [异常处理](#%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86)
  - [自定义操作](#%E8%87%AA%E5%AE%9A%E4%B9%89%E6%93%8D%E4%BD%9C)
  - [实现](#%E5%AE%9E%E7%8E%B0)
    - [写入](#%E5%86%99%E5%85%A5)
  - [1.X与2.X升级说明](#1x%E4%B8%8E2x%E5%8D%87%E7%BA%A7%E8%AF%B4%E6%98%8E)
  - [2.X版本已知问题](#2x%E7%89%88%E6%9C%AC%E5%B7%B2%E7%9F%A5%E9%97%AE%E9%A2%98)
  - [附录](#%E9%99%84%E5%BD%95)
    - [HoloConfig参数说明](#holoconfig%E5%8F%82%E6%95%B0%E8%AF%B4%E6%98%8E)
      - [基础配置](#%E5%9F%BA%E7%A1%80%E9%85%8D%E7%BD%AE)
      - [写入配置](#%E5%86%99%E5%85%A5%E9%85%8D%E7%BD%AE)
      - [查询配置](#%E6%9F%A5%E8%AF%A2%E9%85%8D%E7%BD%AE)
      - [连接配置](#%E8%BF%9E%E6%8E%A5%E9%85%8D%E7%BD%AE)
      - [消费Binlog配置](#%E6%B6%88%E8%B4%B9binlog%E9%85%8D%E7%BD%AE)

## 功能介绍
holo-client适用于大批量数据写入（批量、实时同步至holo）和高QPS点查（维表关联）场景。holo-client基于JDBC实现，使用时请确认实例剩余可用连接数。

- 查看最大连接数
```sql
select instance_max_connections(); -- holo实例版本需大于等于1.3.22，否则请参考官网文档实例规格与连接数的计算方式
```

- 查看已使用连接数
```sql
select count(*) from pg_stat_activity where backend_type='client backend';
```
## holo-client引入
- Maven
```xml
<dependency>
  <groupId>com.alibaba.hologres</groupId>
  <artifactId>holo-client</artifactId>
  <version>2.2.0</version>
</dependency>
```

- Gradle
```
implementation 'com.alibaba.hologres:holo-client:2.2.0'
```

## 连接数说明
- HoloClient最多会同时启动Max(writeThreadSize,readThreadSize)个连接
- idle超过connectionMaxIdleMs会被释放
- 在存活链接不足与处理请求量时，会自动创建新链接

## 数据写入
建议项目中创建HoloClient单例，通过writeThreadSize和readThreadSize控制读写的并发（每并发占用1个JDBC连接，空闲超过connectionMaxIdleMs将被自动回收)
### 写入普通表
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

try (HoloClient client = new HoloClient(config)) {
    TableSchema schema0 = client.getTableSchema("t0");
    Put put = new Put(schema0);
    put.setObject("id", 1);
    put.setObject("name", "name0");
    put.setObject("address", "address0");
    client.put(put); 
    ...
    //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    //client.flush(); 
}catch(HoloClientException e){
}
```
### 写入分区表
注1：若分区已存在，不论DynamicPartition为何值，写入数据都将插入到正确的分区表中；若分区不存在，DynamicPartition设置为true时，将会自动创建不存在的分区，否则抛出异常
注2: 写入分区表在HOLO 0.9及以后版本才能获得较好的性能，0.8建议先写到临时表，再通过insert into xxx select ...的方式写入到分区表
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

config.setDynamicPartition(true); //当分区不存在时，将自动创建分区

try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,region text not null,name text,primary key(id,region)) partition by list(region)
    TableSchema schema0 = client.getTableSchema("t0");
    Put put = new Put(schema0);
    put.setObject("id", 1);
    put.setObject("region", "SH");
    put.setObject("name", "name0");
    client.put(put); 
    ...
    //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    //client.flush(); 
}catch(HoloClientException e){
}
```
### 写入含主键表
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

config.setWriteMode(WriteMode.INSERT_OR_REPLACE);//配置主键冲突时策略

try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,name0 text,address text,primary key(id))
    TableSchema schema0 = client.getTableSchema("t0");
    Put put = new Put(schema0);
    put.setObject("id", 1);
    put.setObject("name0", "name0");
    put.setObject("address", "address0");
    client.put(put); 
    ...
    put = new Put(schema0);
    put.setObject(0, 1);
    put.setObject(1, "newName");
    put.setObject(2, "newAddress");
    client.put(put);
    ...
    //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    //client.flush();
}catch(HoloClientException e){
}
```

### 基于主键删除（DELETE占比提高会降低整体的每秒写入）
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

config.setWriteMode(WriteMode.INSERT_OR_REPLACE);//配置主键冲突时策略

try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,name0 text,address text,primary key(id))
    TableSchema schema0 = client.getTableSchema("t0");
    Put put = new Put(schema0);
    put.getRecord().setType(Put.MutationType.DELETE);
    put.setObject("id", 1);
    client.put(put); 
    ...
    //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    //client.flush();
}catch(HoloClientException e){
}

```
## 数据查询
### 基于完整主键查询
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);
try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,name0 text,address text,primary key(id))
    TableSchema schema0 = client.getTableSchema("t0");
    
    Get get = Get.newBuilder(schema).setPrimaryKey("id", 0).build(); // where id=1;
    client.get(get).thenAcceptAsync((record)->{
        // do something after get result
    });
}catch(HoloClientException e){
}
    
```

### Scan查询
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);
try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,name0 text,address text,primary key(id))
    TableSchema schema0 = client.getTableSchema("t0");
    
    Scan scan = Scan.newBuilder(schema).addEqualFilter("id", 102).addRangeFilter("name", "3", "4").withSelectedColumn("address").build();
    //等同于select address from t0 where id=102 and name>=3 and name<4 order by id; 
    int size = 0;
    try (RecordScanner rs = client.scan(scan)) {
        while (rs.next()) {
            Record record = rs.getRecord();
            //handle record
        }
    }
    //不排序
    scan = Scan.newBuilder(schema).addEqualFilter("id", 102).addRangeFilter("name", "3", "4").withSelectedColumn("address").setSortKeys(SortKeys.NONE).build();
    //等同于select address from t0 where id=102 and name>=3 and name<4; 
    size = 0;
    try (RecordScanner rs = client.scan(scan)) {
        while (rs.next()) {
            Record record = rs.getRecord();
            //handle record
        }
    }
catch(HoloClientException e){
}   
```

## 消费Binlog
Hologres V1.1版本之后，支持使用holo-client进行表的Binlog消费。
Binlog相关知识可以参考文档 [订阅Hologres Binlog](https://help.aliyun.com/document_detail/201024.html) , 使用Holo-client消费Binlog的建表、权限等准备工作和注意事项可以参考 [通过JDBC消费Hologres Binlog](https://help.aliyun.com/document_detail/321431.html) 
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);
config.setBinlogReadBatchSize(128);
config.setBinlogIgnoreDelete(true);
config.setBinlogIgnoreBeforeUpdate(true);
config.setBinlogHeartBeatIntervalMs(5000L);

HoloClient client = new HoloClient(holoConfig);

// 消费binlog的请求，tableName和slotname为必要参数，Subscribe有StartTimeBuilder和OffsetBuilder两种，此处以前者为例
Subscribe subscribe = Subscribe.newStartTimeBuilder(tableName, slotName)
        .setBinlogReadStartTime("2021-01-01 12:00:00")
        .build();

// 创建binlog reader
BinlogShardGroupReader reader = client.binlogSubscribe(subscribe);

BinlogRecord record;
long count = 0;
while ((record = reader.getBinlogRecord()) != null) {
    // 消费到最新
    if (record instanceof BinlogHeartBeatRecord) {
        // do something
        continue;
    }

    // 每1000条数据保存一次消费点位，可以自行选择条件，比如按时间周期等等
    if (count % 1000 == 0) {
        // 保存消费点位，参数表示超时时间，单位为ms
        reader.commit(5000L);
    }

    //handle record
    count++;
}

```
(可选)如需要，可以用OffsetBuilder创建Subscribe，从而为每个shard指定起始消费点位
```java
// 此处shardCount为示例，请替换为所消费表对应的实际数量
int shardCount = 10;
Subscribe.OffsetBuilder subscribeBuilder = Subscribe.newOffsetBuilder(tableName, slotName);
for (int i = 0; i < shardCount; i++) {
    // BinlogOffset通过setSequence指定lsn，通过setTimestamp指定时间，两者同时指定lsn优先级大于时间戳
    subscribeBuilder.addShardStartOffset(i, new BinlogOffset().setSequence(0).setTimestamp("2021-01-01 12:00:00+08"));
}
Subscribe subscribe = subscribeBuilder.build();
BinlogShardGroupReader reader = client.binlogSubscribe(subscribe);
```

## 异常处理
```java
public void doPut(HoloClient client, Put put) throws HoloClientException {
    try{
        client.put(put);
    }catch(HoloClientWithDetailsException e){
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            HoloClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    }catch(HoloClientException e){
        //非HoloClientWithDetailsException的异常一般是fatal的
        throw e;
    }
}

public void doFlush(HoloClient client) throws HoloClientException {
    try{
        client.flush();
    }catch(HoloClientWithDetailsException e){
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            HoloClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    }catch(HoloClientException e){
        //非HoloClientWithDetailsException的异常一般是fatal的
        throw e;
    }
}

```

## 自定义操作
```java
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);
try (HoloClient client = new HoloClient(config)) {
    client.sql(conn -> {
				try (Statement stat = conn.createStatement()) {
					stat.execute("create table t0(id int)");
				}
				return null;
			}).get();
} catch (HoloClientException e) {
}
```
## 实现
### 写入
holoclient写入本质上只将一批Put放入多个队列后，在内存中合并成Batch提交给Worker，worker将batch拆分为若干条sql后提交提交。
1.X版本，写入的sql格式如下
```sql
insert into t0 (c0,c1,c2) values (?,?,?),(?,?,?) on conflict
```
2.X版本，默认写入sql格式如下(需要hologres实例版本>=且不存在数组列)
```sql
insert into to (c0,c1,c2) select unnest(?),unnest(?),unnest(?) on conflict
```

unnest格式相比multi values有如下优点:
- ？个数等于列数，不再会因攒批过大导致？个数超过Short.MAX_VALUE
- batchSize不稳定时，不会产生多个PreparedStatement，节省服务端内存

## 1.X与2.X升级说明
- HoloConfig配置
  - 删除rewriteSqlMaxBatchSize，目前会自动选择最大的Size
  - 删除reWriteBatchedDeletes，自动开启
  - 删除binlogReadStartTime，在Subscribe.Builder中指定
  - 删除binlogReadTimeoutSeconds，在Subscribe.Builder中指定
- 包名
  - org.postgresql.model.*全部切换至com.alibaba.hologres.client.model.*
- 类型
  - Record.type从org.postgresql.core.SqlCommandType切换至com.alibaba.hologres.client.Put.MutationType

## 2.X版本已知问题
- binlog消费时，jdbc无法识别jdbc:postgresql://，必须格式为jdbc:hologres:// bug引入版本2.1.0，bug修复版本2.1.1
- 调用HoloClient.put接口，当holo引擎版本<1.1.38时会错误使用高版本才支持的insert into select unnest模式，导致写入失败 bug引入版本2.1.0，bug修复版本2.1.4
- binlog消费调用commit方法时，尚未消费到数据的shard会错误的将已消费lsn更新为-1 bug引入版本2.1.0，bug修复版本2.1.5
- 调用Put方法时表发生增减列，导致写入失败 bug引入版本1.X， bug修复版本2.1.5
- 当readWriteThread和writeThreadSize都为1时，getTableSchema可能触发死锁 bug引入版本1.X, bug修复版本2.2.0
  
## 附录
### HoloConfig参数说明
#### 基础配置
| 参数名 | 默认值 | 说明 |引入版本|
| --- | --- | --- | --- |
| jdbcUrl | 无 | 必填| 1.2.3 |
| username | 无 | 必填 | 1.2.3 |
| password | 无 | 必填 | 1.2.3 |
| appName | holo-client | jdbc的applicationName参数 | 1.2.9.1 |

#### 通用配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| dynamicPartition | false | 若为true，当分区不存在时自动创建分区 | 1.2.3 |
| useFixedFe | false | 当hologres引擎版本>=1.3，开启FixedFe后，Get/Put将不消耗连接数（beta功能），连接池大小为writeThreadSize和readThreadSize | 2.2.0 |
| connectionSizeWhenUseFixedFe | 1  | 仅useFixedFe=true时生效，表示除了Get/Put之外的调用使用的连接池大小 | 2.2.0 |

#### 写入配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| writeMode | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE当主键冲突时，更新所有列| 1.2.3|
| writeBatchSize | 512 | 每个写入线程的最大批次大小，在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 | 1.2.3 |
| writeBatchByteSize | 2097152（2 * 1024 * 1024） | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 | 1.2.3 |
| writeBatchTotalByteSize | 20971520（20 * 1024 * 1024） | 所有表最大批次bytes大小，单位为Byte，默认20MB，在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交| 1.2.8.1 |
| writeMaxIntervalMs | 10000 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 | 1.2.4 |
| writeFailStrategy | TYR_ONE_BY_ONE | 当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常| 1.2.4|
| writerShardCountResizeIntervalMs | 30s | 主动调用flush时，触发resize，两次resize间隔不短于writerShardCountResizeIntervalMs | 1.2.10.1 |
| flushMaxWaitMs | 60000 | flush操作的最长等待时间  | 1.2.5 |
| inputNumberAsEpochMsForDatetimeColumn | false | 当Number写入Date/timestamp/timestamptz列时，若为true，将number视作ApochMs   | 1.2.5 |
| inputStringAsEpochMsForDatetimeColumn | false | 当String写入Date/timestamp/timestamptz列时，若为true，将String视作ApochMs   | 1.2.6 |
| removeU0000InTextColumnValue | true | 当写入Text/Varchar列时，若为true，剔除字符串中的\u0000 | 1.2.10.1 |
| enableDefaultForNotNullColumn | true | 启用时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00 | 1.2.6 |
| defaultTimeStampText | null | enableDefaultForNotNullColumn=true时，Date/timestamp/timestamptz的默认值 | 1.2.6 |
| useLegacyPutHandler  | false | true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict | 2.0.1 |
| maxRowsPerSql  | Integer.MAX_VALUE | useLegacyPutHandler=false，且通过unnest形式写入时，每条sql的最大行数 | 2.0.1 |
| maxBytesPerSql  | Long.MAX_VALUE | useLegacyPutHandler=false，且通过unnest形式写入时，每条sql的最大字节数 | 2.0.1 |

#### 查询配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| readThreadSize | 1 | 点查并发线程数（每个并发占用1个数据库连接）| 1.2.4|
| readBatchSize | 128 | 点查最大批次大小 | 1.2.3|
| readBatchQueueSize | 256 | 点查请求缓冲队列大小| 1.2.4|
| scanFetchSize | 256 | Scan操作一次fetch的行数 | 1.2.9.1|
| scanTimeoutSeconds | 256 | Scan操作的超时时间 | 1.2.9.1|
| readTimeoutMilliseconds | 0 | Get操作的超时时间，0表示不超时 | 2.1.5 |
| readRetryCount | 1 | Get操作的尝试次数，1表示不重试 | 2.1.5 |

#### 连接配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| retryCount | 3 | 当连接故障时，写入和查询的重试次数 | 1.2.3|
| retrySleepInitMs | 1000 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs | 1.2.3 |
| retrySleepStepMs | 10000 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |1.2.3 |
| connectionMaxIdleMs| 60000 | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放| 1.2.4 |
| metaCacheTTL | 1 min | getTableSchema信息的本地缓存时间 | 1.2.6 |
| metaAutoRefreshFactor | 4 | 当tableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache | 1.2.10.1 |

#### 消费Binlog配置
| 参数名 | 默认值 | 说明 |引入版本 |
| --- | --- | --- | --- |
| binlogReadBatchSize | 1024 | 从每个shard单次获取的Binlog最大批次大小 | 1.2.16.5 |
| binlogHeartBeatIntervalMs | -1 | binlogRead 发送BinlogHeartBeatRecord的间隔.<br>-1表示不发送,<br>当binlog没有新数据，每间隔binlogHeartBeatIntervalMs会下发一条BinlogHeartBeatRecord，此record的timestamp表示截止到这个时间的数据都已经消费完成.| 2.1.0 |
| binlogIgnoreDelete |false| 是否忽略消费Delete类型的binlog | 1.2.16.5 |
| binlogIgnoreBeforeUpdate | false | 是否忽略消费BeforeUpdate类型的binlog | 1.2.16.5 |
| retryCount | 3 | 消费失败时的重试次数，成功消费时重试次数会被重置 | 2.1.5 |
