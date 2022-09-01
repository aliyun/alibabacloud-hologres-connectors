## 依赖hologres-connector-hive-base，实现了Hive 2.x版本的Connector

## 准备工作
- 需要**Hologres 0.9**及以上版本。
- 需要Hive2

### 从中央仓库获取jar
可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。
```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-hive-2.x</artifactId>
    <version>1.1.1</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```
### 自行编译
#### build base jar 并 install 到本地maven仓库
  - -P指定相关版本参数

  ```
  mvn install -pl hologres-connector-hive-base clean package -DskipTests -Phive-2
  ```

#### build jar

  ```
  mvn -pl hologres-connector-hive-2.x clean package -DskipTests
  ```

#### 加载jar包

* 永久：
    * 将hologres-connector-hive-2.x-1.1-SNAPSHOT-jar-with-dependencies.jar放在HiveServer2所在节点的$HIVE_HOME/auxlib目录下（目录不存在就新建）
* session级
    1. 将hologres-connector-hive-2.x-1.1-SNAPSHOT-jar-with-dependencies.jar上传至hdfs
    2. 在hive session中，使用add jar引入jar包。add jar hdfs:....

## 使用示例
以通过hive加载tbl表文件到hologres为例

1.在hologres中创建holo表

```sql
CREATE TABLE hive_customer
(
c_custkey     INT PRIMARY KEY,
c_name        TEXT,
c_address     TEXT,
c_nationkey   INT,
c_phone       TEXT,
c_acctbal     NUMERIC(15,2),
c_mktsegment  TEXT,
c_comment     TEXT
);
```
2.在hive中创建hive本地表，加载tbl文件

```
CREATE EXTERNAL TABLE customer_local
(
    c_custkey     int,
    c_name        string,
    c_address     string,
    c_nationkey   int,
    c_phone       string,
    c_acctbal     decimal(15,2),
    c_mktsegment  string,
    c_comment     string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;

--加载tbl文件到hive本地表
load data local inpath '/path/customer.tbl' overwrite into table customer_local;
```

3.在hive中创建hive外表，外表字段名称一定要和holo一致,类型与holo类型匹配！
* 建外表时可以只选取holo的部分字段，必须包含holo表的全部主键且使用 INSERT_OR_UPDATE 方式写入

```
CREATE EXTERNAL TABLE customer_to_holo
(
    c_custkey     int,
    c_name        string,
    c_address     string,
    c_nationkey   int,
    c_phone       string,
    c_acctbal     decimal(15,2),
    c_mktsegment  string,
    c_comment     string
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db?reWriteBatchedInserts=true",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.write_thread_size" = "12"
);

CREATE EXTERNAL TABLE customer_to_holo_1
(
    c_custkey     int,
    c_name        string,
    c_address     string
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db?reWriteBatchedInserts=true",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.write_thread_size" = "12"
);

CREATE EXTERNAL TABLE customer_to_holo_2
(
    c_custkey     int,
    c_acctbal     decimal(15,2),
    c_phone       string,
    c_mktsegment  string,
    c_comment     string
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db?reWriteBatchedInserts=true",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.write_thread_size" = "12"
);

```

4.写入数据

```sql
--插入单行数据
insert into customer_to_holo values (111,'aaa','bbb',222,'ccc',33.44,'ddd','eee');

--写部分列
insert into customer_to_holo_1 select 1, 'aaa', 'bbb';
insert into customer_to_holo_2 select 1, 123.456, 'foo', 'bar', 'ooohhh';

--从hive本地表写入holo中
insert into customer_to_holo select * from customer_local;
```

5.查询数据

```sql
select * from customer_to_holo where c_custkey = 1;
select * from customer_to_holo_1 where c_custkey = 1;
select * from customer_to_holo_2 where c_custkey = 1;
```

## 参数说明

在建立外表时，使用 TBLPROPERTIES 指定参数，并以"hive.sql."开头，参考上述使用示例建立hologres的外表语句

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| jdbc.driver | 无 | 是 | 必须为`org.postgresql.Driver` |
| jdbc.url | 无 | 是 | Hologres实时数据API的jdbcUrl，包含数据库名称 |
| username | 无 | 是 | 阿里云账号的AccessKey ID |
| password | 无 | 是 | 阿里云账号的Accesskey SECRET |
| table | 无 | 是 | Hologres用于接收数据的表名称 |
| retry_count | 3 | 否 | 当连接故障时，写入和查询的重试次数 |
| retry_sleep_init_ms | 1000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| retry_sleep_step_ms | 10000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| connection_max_idle_ms| 60000 | 否 | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放 |

写参数

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| write_mode | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE 当主键冲突时，更新所有列|
| write_batch_size | 512 | 否 | 每个写入线程的最大批次大小，<br>在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 |
| write_batch_byte_size | 2097152（2 * 1024 * 1024） | 否 | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 |
| use_legacy_put_handler| false | 否 |true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict|
| write_max_interval_ms | 10000 | 否 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交，单位为ms |
| write_fail_strategy | TYR_ONE_BY_ONE | 否 | 当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常 |
| write_thread_size | 1 | 否 | 写入并发线程数（每个并发占用1个数据库连接） |
| dynamic_partition| false|	否 |若为true，写入分区表父表时，当分区不存在时自动创建分区 |

读参数

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| read_thread_size | 1 | 否 | 点查并发线程数（每个并发占用1个数据库连接）|
| read_batch_size | 128 | 否 | 点查最大批次大小 |
| read_batch_queue_size | 256 | 否 | 点查请求缓冲队列大小 |
| scan_fetch_size | 256 | 否 | Scan操作一次fetch的行数 |
| scan_timeout_seconds | 256 | 否 | Scan操作的超时时间 |

## 类型映射
|hive|holo|
|:---:|:---:|
|int|int|
|bigint|int8|
|boolean|bool|
|float|real|
|double|double|
|string|text|
|timestamp|timestamptz|
|date|date|
|decimal|numeric|