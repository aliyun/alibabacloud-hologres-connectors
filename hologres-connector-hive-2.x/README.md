## 依赖hologres-connector-hive-base，实现了Hive 2.x版本的Connector

## 准备工作
- 需要**Hologres 0.9**及以上版本, 建议使用2.1及以上版本。
- 需要Hive2

### 从中央仓库获取jar
可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。
```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-hive-2.x</artifactId>
    <version>1.6.0</version>
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
  mvn install -pl hologres-connector-hive-base clean package -DskipTests -Phive-2
  ```

#### build jar

  ```
  mvn -pl hologres-connector-hive-2.x clean package -DskipTests
  ```

#### 加载jar包

* 永久：
    * 将hologres-connector-hive-2.x-1.6.0-SNAPSHOT-jar-with-dependencies.jar放在HiveServer2所在节点的$HIVE_HOME/auxlib目录下（目录不存在就新建）
* session级
    1. 将hologres-connector-hive-2.x-1.6.0-SNAPSHOT-jar-with-dependencies.jar上传至hdfs
    2. 在hive session中，使用add jar引入jar包。add jar hdfs:....

## 注意事项
hologres hive connector在进行读写时，会使用一定的jdbc连接数。可能受到如下因素影响：
1. hive的mapper数量：connector在map阶段运行，因此mapper的数量会决定hive connector的写入并发。
2. connector每个并发使用的连接数：fixed copy方式写入，每个并发（mapper task）默认使用一个jdbc连接，如果设置了max_writer_number参数，作业最多可能使用max_writer_number个连接。insert 方式写入，每个并发会使用write_thread_size个jdbc连接。
3. 其他方面可能使用的连接数：创建holo外表以及作业启动时，会有schema获取等操作，可能会建立一些短连接。

因此作业使用的最大连接数可以通过如下公示计算：
* fixed copy 模式： max (numberOfMappers * 1, max_writer_number)
* 普通insert模式： numberOfMappers * write_thread_size

> Note：
> 1. 实际上同时执行的task数量还受到hive所在物理机CPU核数等的影响，比如mapper数量为10， 但机器只有6 core，那么同时只会有6个task执行。
> 2. hive mappers的数量主要受到hadoop对文件分块策略的影响，详情可以参考hive相关文档。此处列几个相关参数作为参考。
> * set mapred.max.split.size=256000000;        -- 决定每个map处理的最大的文件大小，单位为B
> * set mapred.min.split.size.per.node=1;       -- 节点中可以处理的最小的文件大小
> * set mapred.min.split.size.per.rack=1;       -- 机架中可以处理的最小的文件大小

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
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.max_writer_number" = "20"
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
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.max_writer_number" = "20"
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
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_customer",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.max_writer_number" = "20"
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

6.脏数据校验

* 目前写入默认使用copy模式，而copy模式是流式写入的，脏数据导致的写入失败异常无法定位到具体行。可以通过设置`dirty_data_check`参数开启脏数据事前校验，从而在写入失败时拿到有问题的数据信息。
* 脏数据事前校验会对写入性能造成一定影响，非排查环节不建议开启.

在holo中建表如下：
```sql
CREATE TABLE hive_copy_test
(
a     INT PRIMARY KEY,
b     varchar(5)
);
```
在hive中创建如下两表，其中`test_copy_to_holo`为默认参数创建，不检查脏数据，`test_copy_to_holo_check`设置了上述参数，可以抛出脏数据具体的行信息。
```
CREATE EXTERNAL TABLE test_copy_to_holo
(
    a     int,
    b     string
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_copy_test",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE"
);

CREATE EXTERNAL TABLE test_copy_to_holo_check
(
    a     int,
    b     string
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "hive_copy_test",
    "hive.sql.write_mode" = "INSERT_OR_UPDATE",
    "hive.sql.dirty_data_check" = "true" --与上表的区别在于此参数
);
```
插入数据
```sql
-- 在varchar(5)类型中插入了长度超过5的字符串
insert into test_copy_to_holo select 1, 'aaaaabbbbb';
```
抛出的异常如下

`invalid value [aaaaabbbbb](java.lang.String) for column b of table hive_copy_test, reason=value too long for type character varying(5)`

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
| fixed_connection_mode| false| 否| 非copy write 模式（insert默认）下，写入和点查不占用连接数（beta功能，需要connector版本>=1.2.0，hologres引擎版本>=1.3）| 
| enable_serverless_computing| false | 否| 是否使用serverless资源, 仅对读取和bulk_load写入有效,详见[serverless computing](https://help.aliyun.com/zh/hologres/user-guide/serverless-computing) | 
| serverless_computing_query_priority|   3   | 否| serverless computing执行优先级  | 
| statement_timeout|  28800000   | 否| query执行的超时时间  | 
写参数

|            参数名             |             默认值             | 是否必填 |                                                                                              说明                                                                                               |
|:--------------------------:|:---------------------------:|:----:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      copy_write_mode       | 实例版本>=1.3.24，默认true，否则false |  否   |                                        是否使用fixed copy方式写入，fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)                                        |
|     copy_write_format      |           binary            |  否   |                                                                                   底层是否走二进制协议，二进制会更快，否则为文本模式                                                                                   |
|         bulk_load          | 实例版本>=2.1.0且无主键表，默认true，否则false |  否   |                                   是否使用bulk load方式写入，需要copy_write_mode设置为true，bulk load即批量copy，相比fixed copy，写入过程中hologres实例的资源负载更低。<br> 推荐Hologres2.1版本且写入无主键表时使用此参数。Hologres2.1优化了无主键表写入能力，无主键表批量写入不产生表锁，改为行锁，可以与Fixed Plan同时进行。<br> 注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0                                    |
|      dirty_data_check      |            false            |  否   |                                                           是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行，RecordChecker会对写入性能造成一定影响，非排查环节不建议开启。                                                            |
|       direct_connect       |      对于可以直连的环境会默认使用直连       |  否   |                                                       copy的瓶颈往往是VIP endpoint的网络吞吐，因此我们会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。                                                        |
|     max_writer_number      |             20              |  否   | copy write模式下hive job可以使用的最大连接数。<br>任务执行过程中，发现当前job使用的连接数小于此参数时，task会创建更多的连接（copy writer）提高写入性能，每个task上限通过max_writer_number_per_task配置。<br>此参数在一定程度上可以避免长尾问题。参数设置为0或者比作业本身的并发task数量就小时，不进行此优化 |
| max_writer_number_per_task |              3              |  否   |                                                             copy write模式下每个task创建的writer上限，默认为3。由于copy writer拥有比价好的吞吐，因此不建议设置的过大                                                              |
|         write_mode         |      INSERT_OR_REPLACE      |  否   |                                  当INSERT目标表为有主键的表时采用不同策略:<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE 当主键冲突时，更新所有列                                   |
|      write_batch_size      |             512             |  否   |                                                               每个写入线程的最大批次大小，<br>在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交                                                                |
|   write_batch_byte_size    |  2097152（2 * 1024 * 1024）   |  否   |                                                  每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                   |
|   use_legacy_put_handler   |            false            |  否   |               true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict                |
|   write_max_interval_ms    |            10000            |  否   |                                                                           距离上次提交超过writeMaxIntervalMs会触发一次批量提交，单位为ms                                                                           |
|    write_fail_strategy     |       TYR_ONE_BY_ONE        |  否   |                                               当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常                                               |
|     write_thread_size      |              1              |  否   |                                                                                    写入并发线程数（每个并发占用1个数据库连接）                                                                                     |
|     dynamic_partition      |            false            |  	否  |                                                                                 若为true，写入分区表父表时，当分区不存在时自动创建分区                                                                                 |

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
|string|text,json,jsonb|
|timestamp|timestamptz|
|date|date|
|decimal|numeric|