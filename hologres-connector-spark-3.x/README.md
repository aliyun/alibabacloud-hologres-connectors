# Spark 3.x版本的Hologres Connector
Spark是用于大规模数据处理的统一分析引擎，Hologres已经与Spark（社区版以及EMR Spark版）高效打通，快速助力企业搭建数据仓库。Hologres提供的Spark Connector，支持在Spark集群创建Hologres Catalog，以外表的方式进行高性能批量读取和导入，相比原生 JDBC 有更好的性能。

<!-- TOC -->
* [准备工作](#准备工作)
* [使用限制和注意事项](#使用限制和注意事项)
  * [连接数计算](#连接数计算)
* [使用Hologres Catalog](#使用hologres-catalog)
  * [初始化 Catalog](#初始化-catalog)
  * [hologres catalog 常用命令](#hologres-catalog-常用命令)
    * [查询所有database](#查询所有database)
    * [查询database下的所有表](#查询database下的所有表)
    * [写入数据](#写入数据)
    * [查询数据](#查询数据)
* [批量导入Hologres](#批量导入hologres)
  * [批量导入无主键表最佳实践](#批量导入无主键表最佳实践)
    * [使用 catalog 写入](#使用-catalog-写入)
    * [使用 spark-sql 创建临时表进行写入](#使用-spark-sql-创建临时表进行写入)
    * [使用 spark-shell 写入](#使用-spark-shell-写入)
    * [使用 pyspark 写入](#使用-pyspark-写入)
  * [批量导入有主键表最佳实践](#批量导入有主键表最佳实践)
* [批量读取Hologres](#批量读取hologres)
    * [使用 catalog 读取](#使用-catalog-读取)
    * [使用 spark-sql 创建临时表进行读取](#使用-spark-sql-创建临时表进行读取)
    * [使用 spark-shell 读取](#使用-spark-shell-读取)
    * [使用 pyspark 读取](#使用-pyspark-读取)
* [参数说明](#参数说明)
    * [通用参数](#通用参数)
    * [写入参数](#写入参数)
    * [读取参数](#读取参数)
* [数据类型映射](#数据类型映射)
<!-- TOC -->

# 准备工作

### 从[maven中央仓库](https://central.sonatype.com/artifact/com.alibaba.hologres/hologres-connector-spark-3.x)获取jar

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。

```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.x</artifactId>
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

- -P指定相关版本参数，本项目使用scala2.12以及spark3.3.1，详情请查看hologres-connector-spark-base子项目README


  ```
  mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.12 -Pspark-3
  ```

打包结果名称为 hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-3.x clean package -DskipTests
  ```

本文为您介绍通过Spark读取或写入数据至Hologres的操作方法。


# 使用限制和注意事项
通过Spark读写Hologres数据，具体限制如下：

+ 实例版本需为V1.3 及以上版本。请在Hologres管控台的实例详情页查看当前实例版本，如实例是V1.3 以下版本，请您使用[自助升级](https://help.aliyun.com/document_detail/359846.htm#section-32k-sdy-wpv)申请升级实例。
+ 需要安装对应版本的Spark环境，能够运行spark-sql、spark-shell或pyspark命令，建议使用 Spark 3.3.1 及以上版本避免出现依赖问题。


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


# 使用Hologres Catalog
1.5.0版本开始, spark-3.x connector 支持Hologres Catalog, 可以方便的以外表的方式读写hologres。在使用连接器进行数据读写时，默认配置通常足以在大多数情况下提供良好的读写性能。具体而言，读取默认使用批量读取模式；写入无主键表时默认使用批量写入（bulk_load），写入有主键表时则默认采用流式写入（stream）模式。如果您对性能有更高的要求，可以根据具体场景，参考本文档下方的读写最佳实践，以进行进一步优化。以下部分将展示如何在 Spark 中使用 Hologres Catalog。
> 目前Hologres Catalog暂不支持创建表。

假设 Hologrse 实例中存在以下 database 和 table

```shell
test_db1
  public.table1
  test_schema.table2
test_db2
  public.table3
```

## 初始化 Catalog
在spark集群中启动spark-sql, 加载connector并指定catalog参数

+ 通过`spark.sql.catalog.<catalog_name>=com.alibaba.hologres.spark3.HoloTableCatalog` 指定catalog实现,其中`<catalog_name>`为 用户自定义的catalog名称，`com.alibaba.hologres.spark3.HoloTableCatalog`为 HoloTableCatalog 的实现路径，不可修改。
+ 通过`spark.sql.catalog.<catalog_name>.<key>=<value>`指定catalog参数, 这里的参数即connector支持的所有参数, 其中username,password和jdbcurl必填,详见下方[参数说明](#参数说明)

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external.username=*** \
--conf spark.sql.catalog.hologres_external.password=*** \
--conf spark.sql.catalog.hologres_external.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1
```

## hologres catalog 常用命令
### 查询所有database
spark 中的 namespace，对应 hologres 中的 database。

```sql
-- 查看hologres catalog中的所有namespace, 即hologres中所有的database
show namespaces in hologres_external;
```
### 查询database下的所有表
```sql
show tables in hologres_external.test_db1;
```

### 查询数据
hologres中的schema和table 组合起来对应 spark 中的表名，因此必须使用``括起来。

```sql
-- 切换到test_db1这个namespace（database）下,查表时可以省略namespace
use hologres_external.test_db1;
select * from `public.table1`;

-- 使用完整 <catalog_name>.<database_name>.`<schema_name.table_name>`
select * from hologres_external.test_db1.`public.table1`;
```

### 写入数据

```sql
insert into hologres_external.test_db1.`public.table1` values(1, 'foo');
```

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
假设 spark 中已经存在一张表作为数据源，本文以csv文件作为源表，spark-sql中创建源表的语句如下。
```sql
CREATE TEMPORARY VIEW csvTable (
    c_custkey bigint,
    c_name string,
    c_address string,
    c_nationkey int,
    c_phone string,
    c_acctbal decimal(15, 2),
    c_mktsegment string,
    c_comment string)
USING csv OPTIONS (
    path "resources/customer.tbl", sep "|"
);
```
假设在 hologres 的test_db1 中，已经存在相同字段的结果表, hologres中创建结果表的语句如下。

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

## 批量导入无主键表最佳实践
批量导入无主键表使用默认配置即可。connector 会在判断表不存在主键时，设置参数`copy_write_mode=bulk_load`。以下是使用不同的方式导入数据到 hologres 无主键表的示例。
### 使用 catalog 写入
+ 初始化 catalog

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external.username=*** \
--conf spark.sql.catalog.hologres_external.password=*** \
--conf spark.sql.catalog.hologres_external.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1
```

+ 从 CSV 源表导入数据至 Hologres 外表

```sql
insert into hologres_external.test_db1.`public.customer_holo_table` select * from csvTable;
```

### 使用 spark-sql 创建临时表进行写入
+ 启动 spark-sql 命令行

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

+ 创建临时表并导入数据

```sql
CREATE TEMPORARY VIEW hologresTable (
    c_custkey bigint,
    c_name string,
    c_address string,
    c_nationkey int,
    c_phone string,
    c_acctbal decimal(15, 2),
    c_mktsegment string,
    c_comment string)
USING hologres OPTIONS (
    jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1",
    username "***", 
    password "***", 
    table "customer_holo_table"
);

-- 目前通过sql创建的hologres view不支持写入部分列（如insert into hologresTable(c_custkey) select c_custkey from csvTable），写入时需要写入DDL中声明的所有字段。如果希望写入部分列，可以建表时仅声明需要写入的字段。
insert into hologresTable select * from csvTable;
```

### 使用 spark-shell 写入
spark-shell 使用 scala 代码而不是 sql 语句来描述数据的写入。

+ 启动 spark-sql 命令行

```python
spark-shell --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
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
pyspark --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
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

## 批量导入有主键表最佳实践
在Hologres中，主键表的批量数据导入默认会触发表锁，限制了多个连接同时进行并发写入的能力。为了解决这一问题，可以通过在Spark中自定义分区策略，依据Hologres表的`distribution_key`字段计算每条数据的shard，将数据重新分为`shardCount`个partition。这种方法每个Spark的Task只负责写一个shard 的数据，通过设置`reshuffle_by_holo_distribution_key`参数，connector 会通过 partition ID 计算得到当前 partition 将要写入的 shard，此时每个写入并发都可以将锁的粒度降低至Shard级别，实现并发写入，提升写入性能。由于每个连接只需要维护很少 shard 的数据，此优化也可以显著降低小文件的数量，降低Hologres 的内存使用。测试表明，对数据进行 Repartiiton 之后再并发写入，相比默认的 Stream 模式写入，可以减少约66.7%的系统负载。

批量导入有主键表,  需要使用到 connector 提供的RepartitionUtil，因此只能通过 spark-shell 方式进行写入。如果写入的表是空表，且不存在主键冲突，设置参数copy_write_mode=bulk_load即可。如果写入的数据存在主键重复的情况，需要设置参数copy_write_mode=bulk_load_on_conflict，此时需要 hologres 实例版本大于 2.2.27。详情参考下方copy_write_mode 的参数说明。
> 批量导入有主键表,  需要使用到 connector 提供的RepartitionUtil，仅支持通过spark-shell方式进行写入。

+ 启动 spark-shell 命令行

```python
spark-shell --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

+ 读取 csv 文件的数据为 DataFrame， 使用connector提供的RepartitionUtil，将输入数据根据hologres的分布键和 shardcount 进行重分布，

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import com.alibaba.hologres.spark.utils.RepartitionUtil

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

// 对数据进行shuffle之后写入到Hologres中
RepartitionUtil.reShuffleThenWrite(csvDf,
  username = "***",
  password = "***",
  url = "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1",
  tableName = "customer_holo_table",
  copyWriteMode = "bulk_load_on_conflict", // 可选,当存在主键冲突时,需要设置为bulk_load_on_conflict,要求必须写入hologres表的所有字段.默认为bulk_load
  writeMode = "insertOrIgnore", // 可选,主键冲突的处理策略.默认为insertOrReplace
)

```

# 批量读取Hologres
spark-connector 1.3.2版本起，支持读取hologres，相比spark默认的jdbc-connector，可以按照hologres表的shard进行并发读取，性能更好。读取的并发也可以通过参数`max_partition_count` 进行限制，最终作业会将读取任务分为`Min(shardCount, max_partition_count)`个读取Task。connector 也支持了 schema 推断，不传入 schema 时，会根据 hologres 表的 schema 推断出 spark 侧的 schema。

spark-connector 1.5.0版本起,  读取 Hologres 表支持了谓词下推，limit 下推以及字段裁剪。同时，支持传入 hologres 的 select query 来读取数据。此版本开始支持了批量模式读取，相比老版本，读取性能提升一个数量级。

### 使用 catalog 读取
+ 初始化 catalog

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external.username=*** \
--conf spark.sql.catalog.hologres_external.password=*** \
--conf spark.sql.catalog.hologres_external.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1 \
--conf spark.sql.catalog.hologres_external.max_partition_count=80
```

+ 从 Hologres 外表读取数据

```shell
-- 支持字段裁剪和谓词下推
select c_custkey,c_name,c_phone from hologres_external.test_db1.`public.customer_holo_table` where c_custkey < 500 limit 10;
```

### 使用 spark-sql 创建临时表进行读取
+ 启动 spark-sql 命令行

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
```

+ 创建临时表并查询数据

```sql
CREATE TEMPORARY VIEW hologresTable
USING hologres OPTIONS (
  jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1",
  username "***", 
  password "***", 
  max_partition_count "10", // 读取Hologres时的最大分区数
  table "customer_holo_table"
);

-- 支持字段裁剪和谓词下推
select c_custkey,c_name,c_phone from hologresTable where c_custkey < 500 limit 10;
```

+ 传入 hologres 的 query 作为数据源，而非 hologres 表

query 相比 table 更加灵活，甚至可以查询两个 holo 表进行 join 的结果。但是通过 query 查询只能是单个并发，如果数据量较大时，仍建议使用 table 的方式读取。

```sql
CREATE TEMPORARY VIEW hologresTable
USING hologres OPTIONS (
  jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db1",
  username "***", 
  password "***", 
  query "select c_custkey,c_name,c_phone from customer_holo_table where c_custkey < 500 limit 10"
);

select * from hologresTable limit 5;
```

### 使用 spark-shell 读取
spark-shell 使用 scala 代码而不是 sql 语句来描述数据的读取。

+ 启动 spark-sql 命令行

```python
spark-shell --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
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
pyspark --jars hologres-connector-spark-3.x-1.5.0-SNAPSHOT-jar-with-dependencies.jar
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
| table | 无 | 写入时必填,读取可以选择使用query参数,详见下方[读取参数](读取参数)) | Hologres 读写数据的表名称 |
| jdbcurl | 无 | 是 | Hologres实时数据API的jdbcUrl，格式为`jdbc:postgresql://<host>:<port>/<db_name>`<br/> 您可以进入[Hologres管理控制台](https://hologram.console.aliyun.com/#/instance)的实例详情页，从实例配置获取主机和端口号<br/> |
| enable_serverless_computing | false | 否 | 是否使用serverless资源, 仅对读取和bulk_load写入有效,详见[serverless computing](https://help.aliyun.com/zh/hologres/user-guide/serverless-computing) |
| serverless_computing_query_priority | 3 | 否 | serverless computing执行优先级 |
| statement_timeout | 28800000 | 否 | query执行的超时时间 |
| retry_count | 3 | 否 | 当连接故障时的重试次数 |


### 写入参数
| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: | :---: |
| copy_write_mode | stream | 否 | 使用copy方式写入的模式，取值如下：      <br/>1. stream（默认值），即[fixed copy](https://help.aliyun.com/zh/hologres/user-guide/accelerate-the-execution-of-sql-statements-by-using-fixed-plans#section-i9l-6b7-bw6)。fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以有更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)。 注：需要connector版本>=1.3.0，hologres引擎版本>=r1.3.34      <br/>2. bulk_load，即批量copy。批量copy相比流式的fixed copy，在rps相同时，holo实例的负载更低，默认仅支持写入无主键表。Hologres2.1优化了无主键表写入能力，无主键表批量写入不产生表锁，改为行锁，可以与Fixed Plan同时进行。如果写入有主键表，需要结合使用 connector 提供的 RepartitionUtil，并开启下方的 reshuffle_by_holo_distribution_key参数。详见本文批量导入有主键表最佳实践一节。  注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0      <br/>3. bulk_load_on_conflict，批量copy 写入有主键表时，支持处理主键重复的情况。<br/>注：需要connector版本>=1.4.2，hologres引擎版本>=r2.2.27      <br/>4. disable，不使用copy而是使用普通的insert方式写入 |
| reshuffle_by_holo_distribution_key | false | 否 | 使用bulkload模式写入有主键表时，必须开启此参数。且写入的数据已经经过repartition，详见本文批量导入有主键表最佳实践一节。   注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0 |
| max_cell_buffer_size | 20971520（20MB） | 否 | 使用copy模式写入时，单个字段的最大长度 |
| copy_write_dirty_data_check | false | 否 | 是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行，会对写入性能造成一定影响，非排查环节不建议开启 |
| direct_connect | 对于可以直连的环境会默认使用直连 | 否 | 批量数据读写的瓶颈往往是endpoint的网络吞吐，因此我们会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。 |
| write_mode | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:   INSERT_OR_IGNORE 当主键冲突时，不写入   INSERT_OR_UPDATE 当主键冲突时，更新相应列   INSERT_OR_REPLACE 当主键冲突时，更新所有列 |
| 以下参数仅copy_write_mode 为 disable 时生效。 | | | |
| dynamic_partition | false | 否 | copy_write_mode 为 disable 时生效。true 表示写入分区表父表时，自动创建不存在的分区 |
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
| push_down_predicate | true | 否 |  是否进行谓词下推，例如查询时的一些过滤条件。  |
| push_down_limit | true | 否 |                                                              是否进行 limit 下推                                                                                                 |
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
