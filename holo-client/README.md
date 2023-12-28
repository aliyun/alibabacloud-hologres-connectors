# 通过holo-client读写Hologres

- [通过holo-client读写Hologres](#通过holo-client读写hologres)
  - [功能介绍](#功能介绍)
  - [holo-client引入](#holo-client引入)
  - [连接数说明](#连接数说明)
  - [数据写入](#数据写入)
    - [写入普通表](#写入普通表)
    - [写入分区表](#写入分区表)
    - [写入含主键表](#写入含主键表)
    - [局部更新主键表](#局部更新主键表)
    - [基于主键删除（DELETE占比提高会降低整体的每秒写入）](#基于主键删除delete占比提高会降低整体的每秒写入)
  - [fixed copy](#fixed-copy)
    - [fixed copy写入普通表](#fixed-copy写入普通表)
    - [fixed copy写入分区表](#fixed-copy写入分区表)
  - [数据查询](#数据查询)
    - [基于完整主键查询](#基于完整主键查询)
    - [Scan查询](#scan查询)
  - [消费Binlog](#消费binlog)
  - [异常处理](#异常处理)
  - [自定义操作](#自定义操作)
  - [实现](#实现)
    - [写入](#写入)
  - [1.X与2.X升级说明](#1x与2x升级说明)
  - [2.X版本已知问题](#2x版本已知问题)
  - [附录](#附录)
    - [HoloConfig参数说明](#holoconfig参数说明)
      - [基础配置](#基础配置)
      - [通用配置](#通用配置)
      - [写入配置](#写入配置)
      - [查询配置](#查询配置)
      - [连接配置](#连接配置)
      - [消费Binlog配置](#消费binlog配置)
    - [参数详解](#参数详解)
      - [writeMode](#writemode)


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
  <version>2.3.0</version>
</dependency>
```

- Gradle
```
implementation 'com.alibaba.hologres:holo-client:2.3.0'
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
} catch (HoloClientException e) {
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
} catch (HoloClientException e) {
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
} catch (HoloClientException e) {
}
```

### 局部更新主键表
```java
// 配置参数,url格式为 jdbc:postgresql://host:port/db
HoloConfig config = new HoloConfig();
config.setJdbcUrl(url);
config.setUsername(username);
config.setPassword(password);

config.setWriteMode(WriteMode.INSERT_OR_UPDATE);//局部更新时，配置主键冲突时策略为INSERT_OR_UPDATE

try (HoloClient client = new HoloClient(config)) {
    //create table t0(id int not null,name0 text,address text,primary key(id))
    TableSchema schema0 = client.getTableSchema("t0");
    /*
    表的字段为id，name0，address 只put id 和 name0两个字段，当主键冲突则更新对应字段，否则写入
    */
    Put put = new Put(schema0);
    put.setObject("id", 1);
    put.setObject("name0", "name0");
    client.put(put); 
    ...
    put = new Put(schema0);
    put.setObject(0, 1);
    put.setObject(1, "newName");
    client.put(put);
    ...
    //强制提交所有未提交put请求；HoloClient内部也会根据WriteBatchSize、WriteBatchByteSize、writeMaxIntervalMs三个参数自动提交
    //client.flush();
} catch (HoloClientException e) {
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
} catch (HoloClientException e) {
}

```

## fixed copy
fixed copy为hologres1.3.x 引入.
fixed copy与HoloClient.put，以及普通copy的差异如下：

| 对比项 | 普通copy | fixed copy | HoloClient.put | 
| --- | --- | --- | --- |
| 锁类型 | 表锁 | 行锁 | 行锁 |
| 数据可见性 | copy命令执行结束后可见 | 写入即可见 | 写入即可见 |
| 事务性 | 有，copy失败回滚 | 无，失败已经插入的数据不会回滚| 有，单行事务|
| 支持的主键冲突策略 | NONE（冲突则报错） | <br>INSERT_OR_UPDATE<br>INSERT_OR_IGNORE | <br>INSERT_OR_UPDATE<br>INSERT_OR_IGNORE<br>INSERT_OR_REPLACE |
| 是否支持delete | 否 | 否 | 是 |
| 性能 | 非常高 | 很高 | 高 |

- 实时写入场景
  - 写入有delete 使用HoloClient.put方法写入.
  - 无delete时，建议使用fixed copy写入，相比HoloClient.put方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批).

对于无delete的实时写入场景，建议都使用fixed copy写入.

### fixed copy写入普通表
```java
public class CopyDemo {
	public static void main(String[] args) throws Exception {
		//注意,jdbcurl里需要是jdbc:hologres
		String jdbcUrl = "jdbc:hologres://host:port/db";
		String user = "";
		String password = "";

		//加载Driver类
		Class.forName("com.alibaba.hologres.org.postgresql.Driver");

		/*
		CREATE TABLE copy_demo (id INT NOT NULL, name TEXT NOT NULL, address TEXT, PRIMARY KEY(id));
		* */
		String tableName = "copy_demo";
		try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
			//所有postgresql相关类均在com.alibaba.hologres包下,如果项目同时引用了holo-client和postgresql,请注意包名
			BaseConnection pgConn = conn.unwrap(BaseConnection.class);
			TableSchema schema = ConnectionUtil.getTableSchema(conn, TableName.valueOf(tableName));

			CopyManager copyManager = new CopyManager(pgConn.unwrap(PgConnection.class));
			// 支持写入部分列, 列在List里的顺序一定要和建表保持一致.
			List<String> columns = new ArrayList<>();
			columns.add("id");
			columns.add("name");
            /*
            INSERT_OR_REPLACE是INSERT_OR_UPDATE的一种特殊情况，考虑到实用性，在fixed copy这里没有做区分；
            这里INSERT_OR_REPLACE语义等同于INSERT_OR_UPDATE，当主键冲突时只对columns里添加的列进行更新；
             */
			String copySql = CopyUtil.buildCopyInSql(schema.getTableName(), columns
					, true /*底层是否走二进制协议，二进制格式的速率更快，否则为文本模式.*/
					, schema.getPrimaryKeys().length > 0 /* 是否包含PK */, WriteMode.INSERT_OR_UPDATE);

			//写入完成/异常后，仅RecordOutputStream调用close方法即可，CopyInOutputStream无需close.
			CopyInOutputStream os = new CopyInOutputStream(copyManager.copyIn(copySql));
			//maxCellBufferSize需要保证能放下一行数据，否则会写入失败.
			try (RecordOutputStream ros = new RecordBinaryOutputStream(os, schema,
					pgConn.unwrap(PgConnection.class), 1024 * 1024 * 10)) {
				for (int i = 0; i < 10; ++i) {
					Record record = new Record(schema);
    
                    /*
                    一定要和buildCopyInSql的columns保持一致，不然会错列.
                      如果一列出现在columns中，这一列一定要调用setObject(index, obj);
                      否则，这一列一定不能调用setObject.
                    */
					record.setObject(0, i);
					record.setObject(1, "name0");

					/*
					 * 如果有脏数据，写入失败的报错很难定位具体行.
					 * 此时可以启用RecordChecker做事前校验，找到有问题的数据.
					 * RecordChecker会对写入性能造成一定影响，非排查环节不建议开启.
					 * */
					//RecordChecker.check(record);
    
                    /*
                    putRecord既将record发送给hologres引擎,并立即返回，
                    引擎会在第一时间尝试写入存储，普遍状态下数据会在5-20ms后可查.
                    当RecordOutputStream的close方法执行完成并且没有任何错误抛出，意味着所有数据均已写入完成可以查询.
                    */
					ros.putRecord(record);
				}
			}

			System.out.println("rows:" + os.getResult());
		}
	}
}
```

### fixed copy写入分区表
fixed copy写入分区表仅可写入分区子表，暂不支持写入分区主表.

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
} catch (HoloClientException e) {
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
    } catch (HoloClientException e) {
    }
}   
```

## 消费Binlog
Hologres V1.1版本之后，支持使用holo-client进行表的Binlog消费。
Binlog相关知识可以参考文档 [订阅Hologres Binlog](https://help.aliyun.com/document_detail/201024.html) , 使用Holo-client消费Binlog的建表、权限等准备工作和注意事项可以参考 [通过JDBC消费Hologres Binlog](https://help.aliyun.com/document_detail/321431.html) 
```java
import com.alibaba.hologres.client.BinlogShardGroupReader;
import com.alibaba.hologres.client.Command;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Subscribe;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.model.binlog.BinlogHeartBeatRecord;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;

import java.util.HashMap;
import java.util.Map;

public class HoloBinlogExample {

  public static BinlogShardGroupReader reader;

  public static void main(String[] args) throws Exception {
    String username = "";
    String password = "";
    String url = "jdbc:postgresql://ip:port/database";
    String tableName = "test_message_src";
    String slotName = "hg_replication_slot_1";

    // 创建client的参数
    HoloConfig holoConfig = new HoloConfig();
    holoConfig.setJdbcUrl(url);
    holoConfig.setUsername(username);
    holoConfig.setPassword(password);
    holoConfig.setBinlogReadBatchSize(128);
    holoConfig.setBinlogIgnoreDelete(true);
    holoConfig.setBinlogIgnoreBeforeUpdate(true);
    holoConfig.setBinlogHeartBeatIntervalMs(5000L);
    HoloClient client = new HoloClient(holoConfig);

    // 获取表的shard数
    int shardCount = Command.getShardCount(client, client.getTableSchema(tableName));

    // 使用map保存每个shard的消费进度, 初始化为0
    Map<Integer, Long> shardIdToLsn = new HashMap<>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      shardIdToLsn.put(i, 0L);
    }

    // 消费binlog的请求，tableName和slotname为必要参数，Subscribe有StartTimeBuilder和OffsetBuilder两种，此处以前者为例
    Subscribe subscribe = Subscribe.newStartTimeBuilder(tableName, slotName)
            .setBinlogReadStartTime("2021-01-01 12:00:00")
            .build();
    // 创建binlog reader
    reader = client.binlogSubscribe(subscribe);

    BinlogRecord record;

    int retryCount = 0;
    long count = 0;
    while(true) {
      try {
        if (reader.isCanceled()) {
          // 根据保存的消费点位重新创建reader
          reader = client.binlogSubscribe(subscribe);
        }
        while ((record = reader.getBinlogRecord()) != null) {
          // 消费到最新
          if (record instanceof BinlogHeartBeatRecord) {
            // do something
            continue;
          }

          // 处理读取到的binlog record，这里只做打印
          System.out.println(record);

          // 处理之后保存消费点位，异常时可以从此点位恢复
          shardIdToLsn.put(record.getShardId(), record.getBinlogLsn());
          count++;

          // 读取成功，重置重试次数
          retryCount = 0;
        }
      } catch (HoloClientException e) {
        if (++retryCount > 10) {
          throw new RuntimeException(e);
        }
        // 发生异常时推荐打印warn级别日志
        System.out.println(String.format("binlog read failed because %s and retry %s times", e.getMessage(), retryCount));

        // 重试期间进行一定时间的等待
        Thread.sleep(5000L * retryCount);

        // 用OffsetBuilder创建Subscribe，从而为每个shard指定起始消费点位
        Subscribe.OffsetBuilder subscribeBuilder = Subscribe.newOffsetBuilder(tableName, slotName);
        for (int i = 0; i < shardCount; i++) {
          // BinlogOffset通过setSequence指定lsn，通过setTimestamp指定时间，两者同时指定lsn优先级大于时间戳
          // 这里根据shardIdToLsn这个Map中保存的消费进度进行恢复
          subscribeBuilder.addShardStartOffset(i, new BinlogOffset().setSequence(shardIdToLsn.get(i)));
        }
        subscribe = subscribeBuilder.build();
        // 关闭reader
        reader.cancel();
      }
    }
  }
}

```

## 异常处理
```java
public void doPut(HoloClient client, Put put) throws HoloClientException {
    try {
        client.put(put);
    } catch (HoloClientWithDetailsException e) {
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            HoloClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    } catch (HoloClientException e) {
        //非HoloClientWithDetailsException的异常一般是fatal的
        throw e;
    }
}

public void doFlush(HoloClient client) throws HoloClientException {
    try {
        client.flush();
    } catch (HoloClientWithDetailsException e) {
        for(int i=0;i<e.size();++i){
            //写入失败的记录
            Record failedRecord = e.getFailRecord(i);
            //写入失败的原因
            HoloClientException cause = e.getException(i);
            //脏数据处理逻辑
        }
    } catch (HoloClientException e) {
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
- binlog消费时，存在内存泄露问题，bug引入版本2.1.0，bug修复版本2.2.10
- 分区表drop column之后，自动创建子分区时，可能拿到错误的分区值，bug引入版本1.X，bug修复版本2.2.11
- 当用户表名中有下划线时，可能获取到错误的字段信息，bug引入版本1.X，bug修复版本2.2.12

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

| 参数名                          | 默认值     | 说明                                                                                                                                                                                       | 引入版本  | 
|------------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|
| dynamicPartition             | false   | 若为true，当分区不存在时自动创建分区                                                                                                                                                                     | 1.2.3 |
| useFixedFe                   | false   | 当hologres引擎版本>=1.3，开启FixedFe后，Get/Put将不消耗连接数（beta功能），连接池大小为writeThreadSize和readThreadSize                                                                                                | 2.2.0 |
| connectionSizeWhenUseFixedFe | 1       | 仅useFixedFe=true时生效，表示除了Get/Put之外的调用使用的连接池大小                                                                                                                                             | 2.2.0 |
| sslMode                      | disable | 是否启动传输加密，详见[Hologres启用SSL传输加密](https://help.aliyun.com/zh/hologres/user-guide/ssl-encrypted-transmission)。取值为disable、require、verify-ca、verify-full，后两种需要通过sslRootCertLocation参数配置CA证书的路径 | 2.3.0 |
| sslRootCertLocation          | 无       | sslMode=verify-ca或verify-full时必填，CA证书的路径                                                                                                                                                 | 2.3.0 |

#### 写入配置
| 参数名                                   | 默认值                        | 说明                                                                                                                                                               | 引入版本     | 
|---------------------------------------|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| writeThreadSize                       | 1                          | 处理HoloClient.put方法请求的最大连接数                                                                                                                                       |
| [writeMode](#writeMode)               | INSERT_OR_REPLACE          | 当INSERT目标表为有主键的表时采用不同策略<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE当主键冲突时，更新所有列                                         | 1.2.3    |
| writeBatchSize                        | 512                        | 每个写入线程的最大批次大小，在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交                                                                                                     | 1.2.3    |
| writeBatchByteSize                    | 2097152（2 * 1024 * 1024）   | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                                       | 1.2.3    |
| writeBatchTotalByteSize               | 20971520（20 * 1024 * 1024） | 所有表最大批次bytes大小，单位为Byte，默认20MB，在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                                              | 1.2.8.1  |
| writeMaxIntervalMs                    | 10000                      | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交                                                                                                                              | 1.2.4    |
| writerShardCountResizeIntervalMs      | 30s                        | 主动调用flush时，触发resize，两次resize间隔不短于writerShardCountResizeIntervalMs                                                                                                | 1.2.10.1 |
| inputNumberAsEpochMsForDatetimeColumn | false                      | 当Number写入Date/timestamp/timestamptz列时，若为true，将number视作ApochMs                                                                                                    | 1.2.5    |
| inputStringAsEpochMsForDatetimeColumn | false                      | 当String写入Date/timestamp/timestamptz列时，若为true，将String视作ApochMs                                                                                                    | 1.2.6    |
| removeU0000InTextColumnValue          | true                       | 当写入Text/Varchar列时，若为true，剔除字符串中的\u0000                                                                                                                           | 1.2.10.1 |
| enableDefaultForNotNullColumn         | true                       | 启用时，not null且未在表上设置default的字段传入null时，将以默认值写入. String 默认“”,Number 默认0,Date/timestamp/timestamptz 默认1970-01-01 00:00:00                                            | 1.2.6    |
| defaultTimeStampText                  | null                       | enableDefaultForNotNullColumn=true时，Date/timestamp/timestamptz的默认值                                                                                               | 1.2.6    |
| useLegacyPutHandler                   | false                      | true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict | 2.0.1    |
| maxRowsPerSql                         | Integer.MAX_VALUE          | useLegacyPutHandler=false，且通过unnest形式写入时，每条sql的最大行数                                                                                                              | 2.0.1    |
| maxBytesPerSql                        | Long.MAX_VALUE             | useLegacyPutHandler=false，且通过unnest形式写入时，每条sql的最大字节数                                                                                                             | 2.0.1    |
| enableAffectedRows                    | false                      | 开启时 若用户用holoclient.sql执行statement.executeUpdate将会返回正确的affectrow计数，但对于行存表进行holoclient.put会有性能下降                                                                   | 2.2.5    |
| enableGenerateBinlog                    | true                       | 关闭时，通过当前holo-client写入的数据不会生成binlog                                                                                                                               | 2.2.11   |
| enableDeduplication                    | true                       | 写入时是否对攒批数据做去重，设置为false表示不会去重，如果数据重复非常严重，性能最差相当于writeBatchSize设置为1的逐条写入.                                                                                          | 2.3.0    |

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

### 参数详解
#### writeMode
- INSERT_OR_INGORE
```sql
-- 当表有PK列时，等价生成如下sql
INSERT INTO t0 (pk, c0, c1, c2) values (?, ?, ?, ?) ON CONFLICT(pk) DO NOTHING;
-- 当表无PK列时，等价生成如下sql
INSERT INTO t0 (c0, c1, c2) values (?, ?, ?);
```
- INSERT_OR_UPDATE
```sql
-- 当表有PK列时，等价生成如下sql
INSERT INTO t0 (pk, c0, c1, c2) values (?, ?, ?) ON CONFLICT(pk) DO UPDATE SET c0 = excluded.c0, c1 = excluded.c1, c2 = excluded.c2;
-- 当表无PK列时，等价生成如下sql
INSERT INTO t0 (c0, c1, c2) values (?, ?, ?);
```
- INSERT_OR_REPLACE
INSERT_OR_REPLACE相比INSERT_OR_UPDATE最大的区别是，UPDATE只有显式调用过Put.setObject的列才会参与到SQL中，而REPLACE模式下，没有调用put.setObject列等同于调用过一次put.setObject(index, null)，所有列都会参与到sql中