# Spark 2.x版本的Hologres Connector
Spark是用于大规模数据处理的统一分析引擎，Hologres已经与Spark（社区版以及EMR Spark版）高效打通，快速助力企业搭建数据仓库。

<!-- TOC -->
* [准备工作](#准备工作)
* [使用限制和注意事项](#使用限制和注意事项)
  * [连接数计算](#连接数计算)
* [批量导入Hologres](#批量导入hologres)
    * [使用 spark-shell 写入](#使用-spark-shell-写入)
    * [使用 pyspark 写入](#使用-pyspark-写入)
* [批量读取Hologres](#批量读取hologres)
  * [使用 spark-shell 读取](#使用-spark-shell-读取)
  * [使用 pyspark 读取](#使用-pyspark-读取)
* [参数说明](#参数说明)
  * [通用参数](#通用参数)
  * [写入参数](#写入参数)
  * [读取参数](#读取参数)
* [数据类型映射](#数据类型映射)
<!-- TOC -->

# 准备工作

### 从[maven中央仓库](https://central.sonatype.com/artifact/com.alibaba.hologres/hologres-connector-spark-2.x)获取jar

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。

```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-2.x</artifactId>
    <version>1.5.0</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

## 自行编译

connector依赖父项目的pom文件，在本项目根目录执行以下命令进行install

```
mvn clean install -N
```

#### build base jar 并 install 到本地maven仓库

- -P指定相关版本参数，本项目使用scala2.11以及spark2.4.0，详情请查看hologres-connector-spark-base子项目README


  ```
  mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.11 -Pspark-2
  ```

打包结果名称为 hologres-connector-spark-2.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-2.x clean package -DskipTests
  ```

本文为您介绍通过Spark读取或写入数据至Hologres的操作方法。


# 使用限制和注意事项
通过Spark读写Hologres数据，具体限制如下：

+ 实例版本需为V1.3 及以上版本。请在Hologres管控台的实例详情页查看当前实例版本，如实例是V1.3 以下版本，请您使用[自助升级](https://help.aliyun.com/document_detail/359846.htm#section-32k-sdy-wpv)申请升级实例。
+ 需要安装对应版本的Spark环境，能够运行spark-sql、spark-shell或pyspark命令，建议使用 Spark 2.4.0 及以上版本避免出现依赖问题。
+ Spark Connector3.x的功能相比2.x更加丰富且简单易用，支持在Spark集群创建Hologres Catalog，以外表的方式进行高性能批量读取和导入。


## 连接数计算
hologres spark connector在进行读写时，会使用一定的jdbc连接数。可能受到如下因素影响：

1. spark的并发，在作业运行时于spark UI处可以看到的同时运行的task数量
2. connector每个并发使用的连接数：copy方式写入，每个并发仅使用一个jdbc连接。insert 方式写入，每个并发会使用write_thread_size个jdbc连接。读取时每个并发使用一个jdbc连接。
3. 其他方面可能使用的连接数：作业启动时，会有schema获取等操作，可能短暂的建立1个连接

因此作业使用的总的连接数可以通过如下公示计算：

+ catalog 查询元数据： 1
+ 读取数据：parallelism * 1 + 1
+ 写入copy模式： parallelism * 1 + 1
+ 写入insert模式： parallelism * write_thread_size + 1

> spark 同时可以运行的task并发可能受到用户设置的参数影响，如`spark.executor.instances`，也可能受到hadoop对文件分块策略的影响，详情可以参考spark相关文档。

# 批量导入Hologres
本小节介绍批量导入数据到 Hologres 中时的最佳实践。数据源使用TPC-H的customer.tbl文件， Spark 可以以 CSV 格式进行读取。customer表字段如下

```sql
c_custkey bigint,
c_name string,
c_address string,
c_nationkey int,
c_phone string,
c_acctbal decimal(15, 2),
c_mktsegment string,
c_comment string
```

customer.tbl 文件内容类似：

```csv
1|Customer#000000001|IVhzIApeRb ot,c,E|15|25-989-741-2988|711.56|BUILDING|to the even, regular platelets. regular, ironic epitaphs nag e
```

假设在 hologres 的test_db1 中，已经存在以下表。

```sql
CREATE TABLE customer_holo_table
(
    C_CUSTKEY    BIGINT ,
    C_NAME       TEXT   ,
    C_ADDRESS    TEXT   ,
    C_NATIONKEY  INT    ,
    C_PHONE      TEXT   ,
    C_ACCTBAL    DECIMAL(15,2) ,
    C_MKTSEGMENT TEXT   ,
    C_COMMENT    TEXT
    --, primary key(C_CUSTKEY)  -- 可以根据需要选择是否创建主键
);
```

* 批量导入有主键表时，使用默认的配置即可。spark3.x的connector支持对数据进行重分布之后写入,可以有效降低写入负载,详情参考spark3.x-connector的文档。
* 批量导入无主键表，仅需要在默认配置的基础上，新增`copy_write_mode=bulk_load`参数。以下是使用不同的方式导入数据到 hologres 无主键表的示例。

### 使用 spark-shell 写入
spark-shell 使用 scala 代码而不是 sql 语句来描述数据的写入。

+ 启动 spark-sql 命令行

```python
spark-shell --jars hologres-connector-spark-2.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

+ 读取 csv 文件的数据为 DataFrame 并写入 hologres

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

// csv源的schema
val schema = StructType(Array(
  StructField("c_custkey", LongType),
  StructField("c_name", StringType),
  StructField("c_address", StringType),
  StructField("c_nationkey", IntegerType),
  StructField("c_phone", StringType),
  StructField("c_acctbal", DecimalType(15, 2)),
  StructField("c_mktsegment", StringType),
  StructField("c_comment", StringType)
))

// 从csv文件读取数据为DataFrame
val csvDf = spark.read.format("csv").schema(schema).option("sep", "|").load("resources/customer.tbl")

// 将读取到的DataFrame写入到Hologres中
csvDf.write
.format("hologres")
.option("username", "***")
.option("password", "***")
.option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1")
.option("table", "customer_holo_table")
.mode(SaveMode.Append)
.save()
```

### 使用 pyspark 写入
与spark-shell类似，pyspark使用源数据创建DataFrame之后调用connector进行写入

```shell
pyspark --jars hologres-connector-spark-2.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

```python
from pyspark.sql.types import *

# csv源的schema
schema = StructType([
  StructField("c_custkey", LongType()),
  StructField("c_name", StringType()),
  StructField("c_address", StringType()),
  StructField("c_nationkey", IntegerType()),
  StructField("c_phone", StringType()),
  StructField("c_acctbal", DecimalType(15, 2)),
  StructField("c_mktsegment", StringType()),
  StructField("c_comment", StringType())
])

# 从csv文件读取数据为DataFrame
csvDf = spark.read.csv("resources/customer.tbl", header=False, schema=schema)

# 将读取到的DataFrame写入到Hologres中
csvDf.write.format("hologres").option(
"username", "***").option(
"password", "***").option(
"jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1").option(
"table", "customer_holo_table").mode(
"append").save()
```

# 批量读取Hologres
spark-connector 1.3.2版本起，支持读取hologres，相比spark默认的jdbc-connector，可以按照hologres表的shard进行并发读取，性能更好。读取的并发也可以通过参数`max_partition_count` 进行限制，最终作业会将读取任务分为`Min(shardCount, max_partition_count)`个读取Task。connector 也支持了 schema 推断，不传入 schema 时，会根据 hologres 表的 schema 推断出 spark 侧的 schema。

### 使用 spark-shell 读取
spark-shell 使用 scala 代码而不是 sql 语句来描述数据的读取。

+ 启动 spark-sql 命令行

```python
spark-shell --jars hologres-connector-spark-2.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

+ 读取 hologres 的数据为 DataFrame

```scala
val readDf = (
  spark.read
    .format("hologres")
    .option("username", "***")
    .option("password", "***")
    .option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1")
    .option("table", "customer_holo_table")
    .option("max_partition_count", "80") // 读取Hologres时的最大分区数
    .load()
    .filter("c_custkey < 500")
)

readDf.select("c_custkey", "c_name", "c_phone").show(10)
```

### 使用 pyspark 读取
与spark-shell类似，pyspark使用源数据创建DataFrame之后调用connector进行写入

```scala
pyspark --jars hologres-connector-spark-2.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

```python
# 将读取到的DataFrame写入到Hologres中
readDf = spark.read.format("hologres").option(
"username", "***").option(
"password", "***").option(
"jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1").option(
"table", "customer_holo_table").option(
"max_partition_count", "10").load()

readDf.select("c_custkey", "c_name", "c_phone").show(10)
```


# 参数说明
### 通用参数
| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: | :---: |
| username | 无 | 是 | 当前账号的AccessKey ID。您可以单击[AccessKey 管理](https://usercenter.console.aliyun.com/?spm=5176.2020520153.nav-right.dak.3bcf415dCWGUBj#/manage/ak)来获取。<br/>建议您使用环境变量的方式调用用户名和密码，降低密码泄露风险。 |
| password | 无 | 是 | 当前账号的AccessKey Secret。您可以单击[AccessKey 管理](https://usercenter.console.aliyun.com/?spm=5176.2020520153.nav-right.dak.3bcf415dCWGUBj#/manage/ak)来获取。<br/>建议您使用环境变量的方式调用用户名和密码，降低密码泄露风险。 |
| table | 无 | 写入时必填,读取可以选择使用query参数,详见下方[读取参数](读取参数) | Hologres 读写数据的表名称 |
| jdbcurl | 无 | 是 | Hologres实时数据API的jdbcUrl，格式为`jdbc:postgresql://<host>:<port>/<db_name>`<br/> 您可以进入[Hologres管理控制台](https://hologram.console.aliyun.com/#/instance)的实例详情页，从实例配置获取主机和端口号<br/> |
| enable_serverless_computing | false | 否 | 是否使用serverless资源, 仅对读取和bulk_load写入有效,详见[serverless computing](https://help.aliyun.com/zh/hologres/user-guide/serverless-computing) |
| serverless_computing_query_priority | 3 | 否 | serverless computing执行优先级 |
| statement_timeout | 28800000 | 否 | query执行的超时时间 |
| retry_count | 3 | 否 | 当连接故障时的重试次数 |


### 写入参数
| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: | :---: |
| copy_write_mode | stream | 否 | 使用copy方式写入的模式，取值如下：      <br/>1. stream（默认值），即[fixed copy](https://help.aliyun.com/zh/hologres/user-guide/accelerate-the-execution-of-sql-statements-by-using-fixed-plans#section-i9l-6b7-bw6)。fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以有更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)。 注：需要connector版本>=1.3.0，hologres引擎版本>=r1.3.34      <br/>2. bulk_load，即批量copy。批量copy相比流式的fixed copy，在rps相同时，holo实例的负载更低，默认仅支持写入无主键表。Hologres2.1优化了无主键表写入能力，无主键表批量写入不产生表锁，改为行锁，可以与Fixed Plan同时进行。如果写入有主键表，需要结合使用 connector 提供的 RepartitionUtil，并开启下方的 reshuffle_by_holo_distribution_key参数。详见本文批量导入有主键表最佳实践一节。  注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0      <br/>3. bulk_load_on_conflict，批量copy 写入有主键表时，支持处理主键重复的情况。<br/>注：需要connector版本>=1.4.2，hologres引擎版本>=r2.2.27      <br/>4. disable，不使用copy而是使用普通的insert方式写入 |
| max_cell_buffer_size | 20971520（20MB） | 否 | 使用copy模式写入时，单个字段的最大长度 |
| copy_write_dirty_data_check | false | 否 | 是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行，会对写入性能造成一定影响，非排查环节不建议开启 |
| direct_connect | 对于可以直连的环境会默认使用直连 | 否 | 批量数据读写的瓶颈往往是endpoint的网络吞吐，因此我们会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。 |
| 以下参数仅copy_write_mode 为 disable 时生效。 | | | |
| dynamic_partition | false | 否 | copy_write_mode 为 disable 时生效。true 表示写入分区表父表时，自动创建不存在的分区 |
| write_mode | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:   INSERT_OR_IGNORE 当主键冲突时，不写入   INSERT_OR_UPDATE 当主键冲突时，更新相应列   INSERT_OR_REPLACE 当主键冲突时，更新所有列 |
| write_batch_size | 512 | 否 | 每个写入线程的最大批次大小，   在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 |
| write_batch_byte_size | 2097152（2 * 1024 * 1024） | 否 | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，   在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 |
| use_legacy_put_handler | false | 否 | true时，写入sql格式为insert into xxx(c0,c1,…) values (?,?,…),… on conflict; false时优先使用sql格式为insert into xxx(c0,c1,…) select unnest(?),unnest(?),… on conflict |
| write_max_interval_ms | 10000 | 否 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| write_fail_strategy | TYR_ONE_BY_ONE | 否 | 当发生写失败时的重试策略:   TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出   NONE 直接抛出异常 |
| write_thread_size | 1 | 否 | 写入并发线程数（每个并发占用1个数据库连接） |


### 读取参数
| 参数名 | 默认值  | 是否必填 |                                                                               说明                                                                                |
| :---: |:----:| :---: |:---------------------------------------------------------------------------------------------------------------------------------------------------------------:|
| max_partition_count |  80  | 否 |                                        将读取的Hologres表分为多个分区，每个分区对应一个spark task。如果holo表的shardcount小于此参数,分区数量最多为shardcount。                                        |
| bulk_read | true | 否 | 设置为 true 时表示使用批量读取，设置为 false 时，使用 select 查询方式读取数据。<br/>> 此参数建议设置为 ture，批量读取的性能是 select 方式的 10 倍以上。 <br/>> bulk_read 设置为 true 时，不支持读取 hologres 中的 jsonb 类型。<br/> |
| scan_batch_size | 256  | 否 |                                                       bulk_read 设置为 false 生效。读取Hologres时 scan操作一次fetch的行数                                                       |
| scan_timeout_seconds |  60  | 否 |                                                          bulk_read 设置为 false 生效。读取Hologres时scan操作的超时时间                                                          |
| query |  无   | 否 |                        使用传入的query去读取hologres, 此参数与table参数二者只能设置一个.   需要注意的是,使用query读取时只能单task读取,使用table方式读取时,会根据holo表的shardcount分为多个task                        |

# 数据类型映射
| Spark类型 | Hologres类型                        |
| --- |-----------------------------------|
| ShortType | SMALLINT                          |
| IntegerType | INT                               |
| LongType | BIGINT                            |
| StringType | TEXT                              |
| StringType | JSON                              |
| StringType | JSONB<br> 批量读取暂时不支持 JSONB 类型 |
| DecimalType | NUMERIC(38, 18)                   |
| BooleanType | BOOL                              |
| DoubleType | DOUBLE PRECISION                  |
| FloatType | FLOAT                             |
| TimestampType | TIMESTAMPTZ                       |
| DateType | DATE                              |
| BinaryType | BYTEA                             |
| BinaryType | ROARINGBITMAP                     |
| ArrayType(IntegerType) | int4[]                            |
| ArrayType(LongType) | int8[]                            |
| ArrayType(FloatType | float4[]                          |
| ArrayType(DoubleType) | float8[]                          |
| ArrayType(BooleanType) | boolean[]                         |
| ArrayType(StringType) | text[]                            |
