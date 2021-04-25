# 通过holo-client读写Hologres

- [通过holo-client读写Hologres](#通过holo-client读写hologres)
  - [功能介绍](#功能介绍)
  - [holo-client引入](#holo-client引入)
  - [连接数说明](#连接数说明)
  - [数据读写](#数据读写)
    - [写入普通表](#写入普通表)
    - [写入分区表](#写入分区表)
    - [写入含主键表](#写入含主键表)
    - [基于主键删除（DELETE占比提高会降低整体的每秒写入）](#基于主键删除delete占比提高会降低整体的每秒写入)
    - [基于完整主键查询](#基于完整主键查询)
    - [Scan查询](#scan查询)
  - [异常处理](#异常处理)
  - [自定义操作](#自定义操作)
  - [已知问题](#已知问题)
  - [附录](#附录)
    - [HoloConfig参数说明](#holoconfig参数说明)
      - [基础配置](#基础配置)
      - [写入配置](#写入配置)
      - [查询配置](#查询配置)
      - [连接配置](#连接配置)

## 功能介绍
holo-client适用于大批量数据写入（批量、实时同步至holo）和高QPS点查（维表关联）场景。holo-client基于JDBC实现，使用时请确认实例剩余可用连接数。

- 查看最大连接数
```sql
show max_connections;
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
  <version>1.2.13.1</version>
</dependency>
```

- Gradle
```
implementation 'com.alibaba.hologres:holo-client:1.2.10.3'
```

## 连接数说明
- HoloClient最多会同时启动Max(writeThreadSize,readThreadSize)个连接
- idle超过connectionMaxIdleMs会被释放
- 在存活链接不足与处理请求量时，会自动创建新链接

## 数据读写
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
    client.flush(); //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
catch(HoloClientException e){
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
    client.flush(); //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
catch(HoloClientException e){
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
    client.flush();//强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
catch(HoloClientException e){
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
    put.getRecord().setType(SqlCommandType.DELETE);
    put.setObject("id", 1);
    client.put(put); 
    ...
    client.flush();//强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
catch(HoloClientException e){
}

```
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
catch(HoloClientException e){
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
    //等同于select address from t0 where id=102 and name>=3 and name<4; 
    int size = 0;
    try (RecordScanner rs = client.scan(scan)) {
        while (rs.next()) {
            Record record = rs.getRecord();
            //handle record
        }
    }
catch(HoloClientException e){
}   
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
catch(HoloClientException e){
}
```

## 已知问题
- INSERT_OR_IGNORE和INSERT_OR_UPDATE模式下，insert和delete不保序  bug引入版本1.2.8，bug修复版本1.2.10.3

## 附录
### HoloConfig参数说明
#### 基础配置
| 参数名 | 默认值 | 说明 |引入版本|
| --- | --- | --- | --- |
| jdbcUrl | 无 | 必填| 1.2.3 |
| username | 无 | 必填 | 1.2.3 |
| password | 无 | 必填 | 1.2.3 |
| appName | holo-client | jdbc的applicationName参数 | 1.2.9.1 |

#### 写入配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| dynamicPartition | false | 若为true，当分区不存在时自动创建分区 | 1.2.3 |
| writeMode | INSERT_OR_REPLACE | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE当主键冲突时，更新所有列| 1.2.3|
| writeBatchSize | 512 | 每个写入线程的最大批次大小，在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 | 1.2.3 |
| writeBatchByteSize | 2MB | 每个表最大批次bytes大小，在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 | 1.2.3 |
| writeBatchTotalByteSize | 20MB |所有表最大批次bytes大小，在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交| 1.2.8.1 |
| writeMaxIntervalMs | 10000 ms | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 | 1.2.4 |
| writeFailStrategy | TYR_ONE_BY_ONE | 当某一批次提交失败时，会将批次内的记录逐条提交（保序），单条提交失败的记录将会跟随异常HoloClientWithDatailsException被抛出| 1.2.4|
| writerShardCountResizeIntervalMs | 30s | 主动调用flush时，触发resize，两次resize间隔不短于writerShardCountResizeIntervalMs | 1.2.10.1 |
| flushMaxWaitMs | 60000 ms | flush操作的最长等待时间  | 1.2.5 |
| inputNumberAsEpochMsForDatetimeColumn | false | 当Number写入Date/timestamp/timestamptz列时，若为true，将number视作ApochMs   | 1.2.5 |
| inputStringAsEpochMsForDatetimeColumn | false | 当String写入Date/timestamp/timestamptz列时，若为true，将String视作ApochMs   | 1.2.6 |
| removeU0000InTextColumnValue | true | 当写入Text/Varchar列时，若为true，剔除字符串中的\u0000 | 1.2.10.1 |
| enableDefaultForNotNullColumn | true | 启用时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00 | 1.2.6 |
| defaultTimeStampText | null | enableDefaultForNotNullColumn=true时，Date/timestamp/timestamptz的默认值 | 1.2.6 |


#### 查询配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| readThreadSize | 1 | 点查并发线程数（每个并发占用1个数据库连接）| 1.2.4|
| readBatchSize | 128 | 点查最大批次大小 | 1.2.3|
| readBatchQueueSize | 256 | 点查请求缓冲队列大小| 1.2.4|
| scanFetchSize | 256 | Scan操作一次fetch的行数 | 1.2.9.1|
| scanTimeoutSeconds | 256 | Scan操作的超时时间 | 1.2.9.1|

#### 连接配置
| 参数名 | 默认值 | 说明 |引入版本| 
| --- | --- | --- | --- |
| retryCount | 3 | 当连接故障时，写入和查询的重试次数 | 1.2.3|
| retrySleepInitMs | 1000 ms | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs | 1.2.3 |
| retrySleepStepMs | 10*1000 ms | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |1.2.3 |
| connectionMaxIdleMs| 60000 ms | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放| 1.2.4 |
| metaCacheTTL | 1 min | getTableSchema信息的本地缓存时间 | 1.2.6 |
| metaAutoRefreshFactor | 4 | 当tableSchema cache剩余存活时间短于 metaCacheTTL/metaAutoRefreshFactor 将自动刷新cache | 1.2.10.1 |
