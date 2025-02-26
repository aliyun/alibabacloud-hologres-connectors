# Spark 3.x版本的Hologres Connector
Spark是用于大规模数据处理的统一分析引擎，Hologres已经与Spark（社区版以及EMR Spark版）高效打通，快速助力企业搭建数据仓库。Hologres提供的Spark Connector，支持在Spark集群创建Hologres Catalog，以外表的方式进行高性能批量读取和导入，相比原生 JDBC 有更好的性能。

<!-- TOC -->
* [Spark 3.x版本的Hologres Connector](#spark-3x版本的hologres-connector)
* [使用限制与注意事项](#使用限制与注意事项)
* [准备工作](#准备工作)
  * [自行编译](#自行编译)
  * [下载Release包](#下载release包)
* [Hologres Catalog 介绍](#hologres-catalog-介绍)
  * [初始化 Hologres Catalog](#初始化-hologres-catalog)
  * [Hologres Catalog 常用命令](#hologres-catalog-常用命令)
* [导入Hologres](#导入hologres)
  * [使用 Spark-SQL 写入](#使用-spark-sql-写入)
  * [使用 DataFrame 写入](#使用-dataframe-写入)
* [读取Hologres](#读取hologres)
  * [使用 Spark-SQL 读取](#使用-spark-sql-读取)
  * [读取 Hologres 数据为 DataFrame](#读取-hologres-数据为-dataframe)
* [参数说明](#参数说明)
    * [通用参数](#通用参数)
    * [写入参数](#写入参数)
    * [读取参数](#读取参数)
* [数据类型映射](#数据类型映射)
* [连接数计算](#连接数计算)
<!-- TOC -->

# 使用限制与注意事项
通过Spark读写Hologres数据，具体限制如下：

+ Hologres 实例版本需为V1.3 及以上。请在Hologres管控台的实例详情页查看当前实例版本，如实例是 V1.3 以下版本，请您使用[自助升级](https://help.aliyun.com/document_detail/359846.htm#section-32k-sdy-wpv)或通过搜索（钉钉群号：32314975）加入实时数仓Hologres交流群申请升级实例。
+ 需要安装对应版本的Spark环境，能够运行spark-sql、spark-shell或pyspark命令，建议使用 Spark 3.3.0 及以上版本避免出现依赖问题，并体验更丰富的能力。


# 准备工作

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

打包结果名称为 hologres-connector-spark-3.x-1.5.1-SNAPSHOT-jar-with-dependencies.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-3.x clean package -DskipTests
  ```
## 下载Release包

+ Spark 读写 Hologres时需要引用connector的JAR包，最新的依赖可以从[maven中央仓库](https://central.sonatype.com/artifact/com.alibaba.hologres/hologres-connector-spark-3.x)下载，在项目中使用可以参照如下pom文件进行配置。

```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.x</artifactId>
    <version>1.5.1.1</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```



# Hologres Catalog 介绍
hologres-connector-spark 1.5.1.1 版本开始支持Hologres Catalog, 可以方便的以外表的方式读写hologres。Spark 中每个 hologres catalog 对应 hologres 中的 database，catalog 中的 namespace 对应 database 内的 schema。 以下部分将展示如何在 Spark 中使用 Hologres Catalog。

> 目前Hologres Catalog 暂不支持创建表。
>

假设 Hologrse 实例中存在以下 database 和 table：

```shell
test_db
    public.test_table1
    public.test_table2
    Test_Schema.test_table3
```

## 初始化 Hologres Catalog
在spark集群中启动spark-sql, 加载connector并指定catalog参数

+ 通过`spark.sql.catalog.<catalog_name>=com.alibaba.hologres.spark3.HoloTableCatalog` 指定catalog 名称,其中`<catalog_name>`为用户自定义的catalog名称，`com.alibaba.hologres.spark3.HoloTableCatalog`为 HoloTableCatalog 的实现路径，不可修改。
+ 通过`spark.sql.catalog.<catalog_name>.<key>=<value>`指定catalog参数, 这里的参数即connector支持的所有参数, 其中username,password和jdbcurl必填，jdbcurl 的格式为`jdbc:postgresql://<host>:<port>/<database>`，url 中的 database 即为此 Catalog 对应的 database。其他参数请见下方[参数说明](#X7UZc)。

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.1.1-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external_test_db=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external_test_db.username=*** \
--conf spark.sql.catalog.hologres_external_test_db.password=*** \
--conf spark.sql.catalog.hologres_external_test_db.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db
```

## Hologres Catalog 常用命令
### 加载 Hologres catalog
Spark 中的 Hologres catalog 完全对应一个 Hologres 的 database， 使用过程中无法更改。

```shell
use hologres_external_test_db;
```

### 查询所有 namespace
Spark 中的 namespace，对应 Hologres 中的 schema，默认为 public，使用过程中可以使用 use 调整默认的 schema。

```sql
-- 查看hologres catalog中的所有namespace, 即hologres中所有的schema
show namespaces;
```

### 查询 namespace 下的所有表
```shell
show tables;
```

```sql
use Test_Schema;
show tables;
-- 或者使用 
show tables in Test_Schema;
```

### 读写表
使用 SELECT 和 INSERT 语句对 Catalog 中的 Hologres 外表进行读写。

```sql
SELECT * FROM public.test_table1;
INSERT INTO Test_Schema.test_table3 SELECT * FROM public.test_table1;
```



# 导入Hologres
本小节介绍如何使用 spark-connector 进行数据的导入。数据源使用TPC-H 测试集的customer.tbl文件， Spark 可以以 CSV 格式进行读取。customer表字段如下

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

假设在 hologres 的 test_db 中，已经存在相同字段的结果表, hologres中创建表的语句如下。

```sql
CREATE TABLE customer_holo_table
(
  c_custkey    BIGINT ,
  c_name       TEXT   ,
  c_address    TEXT   ,
  c_nationkey  INT    ,
  c_phone      TEXT   ,
  c_acctbal    DECIMAL(15,2) ,
  c_mktsegment TEXT   ,
  c_comment    TEXT
);
```

## 使用 Spark-SQL 写入
使用 Spark-SQL 时，通过 Catalog 加载 Hologres 表的元数据更加方便，也可以通过创建临时表的方式声明一个 Hologres 表。

> hologres-connector-spark 1.5.1.1 以下版本，不支持 Catalog，只能通过创建临时表的方式声明 Hologres 表。
>

+ [初始化 Hologrse Catalog](#KBjJv)

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.1.1-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external_test_db=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external_test_db.username=*** \
--conf spark.sql.catalog.hologres_external_test_db.password=*** \
--conf spark.sql.catalog.hologres_external_test_db.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db
```

+ 从 CSV 源表导入数据至 Hologres 外表

> 注：Spark [INSERT INTO](https://archive.apache.org/dist/spark/docs/3.5.1/sql-ref-syntax-dml-insert-table.html#parameters)不支持通过column list指定部分列进行写入，如使用`INSERT INTO hologresTable(c_custkey) SELECT c_custkey FROM csvTable`来表示只写入c_custkey 这一个字段。如果希望写入部分字段，可以使用`CREATE TEMPORARY VIEW`  的方式声明仅包含部分字段的 hologres 临时表。
>

```sql
-- 加载hologres catalog
use hologres_external_test_db;

-- 创建csv数据源
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

-- 将csv表的数据写入到hologers中
INSERT INTO public.customer_holo_table SELECT * FROM csvTable;
```



```sql
-- 创建csv数据源
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

-- 创建hologres临时表
CREATE TEMPORARY VIEW hologresTable (
    c_custkey bigint,
    c_name string,
    c_phone string)
USING hologres OPTIONS (
    jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db",
    username "***", 
    password "***", 
    table "customer_holo_table"
);

INSERT INTO hologresTable SELECT c_custkey,c_name,c_phone FROM csvTable;
```



## 使用 DataFrame 写入
如果使用 spark-shell、pyspark 等开发 spark 作业，可以调用 DataFrame 的 write 接口来进行写入。不同语言将读取 csv 文件的数据为 DataFrame 并写入 hologres 的示例如下。需要正确加载 connector 的 jar 包。

```shell
-- 运行时正确加载依赖
spark-shell --jars hologres-connector-spark-3.x-1.5.1.1-SNAPSHOT-jar-with-dependencies.jar
pyspark --jars hologres-connector-spark-3.x-1.5.1.1-SNAPSHOT-jar-with-dependencies.jar
```

### Scala
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
.option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db")
.option("table", "customer_holo_table")
.mode(SaveMode.Append)
.save()
```


### Java
```java
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SaveMode;

// csv源的schema
List<StructField> asList =
        Arrays.asList(
                DataTypes.createStructField("c_custkey", DataTypes.LongType, true),
                DataTypes.createStructField("c_name", DataTypes.StringType, true),
                DataTypes.createStructField("c_address", DataTypes.StringType, true),
                DataTypes.createStructField("c_nationkey", DataTypes.IntegerType, true),
                DataTypes.createStructField("c_phone", DataTypes.StringType, true),
                DataTypes.createStructField("c_acctbal", new DecimalType(15, 2), true),
                DataTypes.createStructField("c_mktsegment", DataTypes.StringType, true),
                DataTypes.createStructField("c_comment", DataTypes.StringType, true));
StructType schema = DataTypes.createStructType(asList);


// 从csv文件读取数据为DataFrame
Dataset<Row> csvDf = spark.read.format("csv").schema(schema).option("sep", "|").load("resources/customer.tbl");

// 将读取到的DataFrame写入到Hologres中
csvDf.write
.format("hologres")
.option("username", "***")
.option("password", "***")
.option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db")
.option("table", "customer_holo_table")
.mode(SaveMode.Append)
.save();
```
### Python
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
    "jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db").option(
    "table", "customer_holo_table").mode(
    "append").save()
```



# 读取Hologres
spark-connector 1.3.2版本起，支持读取hologres，相比spark默认的jdbc-connector，可以按照hologres表的shard进行并发读取，性能更好。读取的并发与表的 shard 数有关，也可以通过参数`max_task_count` 进行限制，最终作业会生成`Min(shardCount, max_task_count)`个读取Task。connector 也支持了 schema 推断，不传入 schema 时，会根据 hologres 表的 schema 推断出 spark 侧的 schema。

spark-connector 1.5.0版本起,  读取 Hologres 表支持了谓词下推，limit 下推以及字段裁剪。同时，支持传入 hologres 的 select query 来读取数据。此版本开始支持了批量模式读取，相比老版本，读取性能提升一个数量级。



## 使用 Spark-SQL 读取
使用 Spark-SQL 时，通过 Catalog 加载 Hologres 表的元数据更加方便，也可以通过创建临时表的方式声明一个 Hologres 表。

> hologres-connector-spark 1.5.1.1 以下版本，不支持 Catalog，只能通过创建临时表的方式声明 Hologres 表。
>

+ [初始化 Hologrse Catalog](#KBjJv)

```shell
spark-sql --jars hologres-connector-spark-3.x-1.5.1.1-jar-with-dependencies.jar \
--conf spark.sql.catalog.hologres_external_test_db=com.alibaba.hologres.spark3.HoloTableCatalog \
--conf spark.sql.catalog.hologres_external_test_db.username=*** \
--conf spark.sql.catalog.hologres_external_test_db.password=*** \
--conf spark.sql.catalog.hologres_external_test_db.jdbcurl=jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db
```

+ 从 Hologres 读取数据
  - Hologres-connector-spark 支持传入 hologres 的 query 作为数据源，query 相比 table 更加灵活，比如可以查询两个 holo 表进行 join 的结果。但是通过 query 查询时仅支持单个并发读取。

```sql
-- 加载hologres catalog
use hologres_external_test_db;

-- 读取hologres表，支持字段裁剪和谓词下推
SELECT c_custkey,c_name,c_phone FROM public.customer_holo_table WHERE c_custkey < 500 LIMIT 10;
```

```sql
CREATE TEMPORARY VIEW hologresTable
USING hologres OPTIONS (
  jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db",
  username "***", 
  password "***", 
  max_task_count "80", // Hologres表最多分为多少个task进行读取
  table "customer_holo_table"
);

-- 支持字段裁剪和谓词下推
SELECT c_custkey,c_name,c_phone FROM hologresTable WHERE c_custkey < 500 LIMIT 10;
```

```sql
CREATE TEMPORARY VIEW hologresTable
USING hologres OPTIONS (
  jdbcurl "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db",
  username "***", 
  password "***", 
  query "select c_custkey,c_name,c_phone from customer_holo_table where c_custkey < 500 limit 10"
);

SELECT * FROM hologresTable LIMIT 5;
```

## 读取 Hologres 数据为 DataFrame
如果使用 spark-shell、pyspark 等开发 spark 作业，可以调用 spark 的 read 接口将数据读取为 DataFrame 以进行后续的计算。不同语言读取 Hologres 表为 DataFrame 的示例如下。使用时需要正确加载 connector 的 jar 包。

```shell
-- 运行时正确加载依赖
spark-shell --jars hologres-connector-spark-3.x-1.5.1.1-SNAPSHOT-jar-with-dependencies.jar
pyspark --jars hologres-connector-spark-3.x-1.5.1.1-SNAPSHOT-jar-with-dependencies.jar
```

### Scala
```scala
val readDf = (
  spark.read
    .format("hologres")
    .option("username", "***")
    .option("password", "***")
    .option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db")
    .option("table", "customer_holo_table")
    .option("max_task_count", "80") // Hologres表最多分为多少个task进行读取
    .load()
    .filter("c_custkey < 500")
)

readDf.select("c_custkey", "c_name", "c_phone").show(10)
```

### Java
```java
Dataset<Row> readDf = (
  spark.read
    .format("hologres")
    .option("username", "***")
    .option("password", "***")
    .option("jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db")
    .option("table", "customer_holo_table")
    .option("max_task_count", "80") // Hologres表最多分为多少个task进行读取
    .load()
    .filter("c_custkey < 500")
);

readDf.select("c_custkey", "c_name", "c_phone").show(10);
```

### Python
```python
readDf = spark.read.format("hologres").option(
"username", "***").option(
"password", "***").option(
"jdbcurl", "jdbc:postgresql://hgpostcn-cn-***-vpc-st.hologres.aliyuncs.com:80/test_db").option(
"table", "customer_holo_table").option(
"max_task_count", "80").load()

readDf.select("c_custkey", "c_name", "c_phone").show(10)
```





# 参数说明
### 通用参数
| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: | --- |
| username | 无 | 是 | 当前账号的AccessKey ID。您可以单击[AccessKey 管理](https://usercenter.console.aliyun.com/?spm=5176.2020520153.nav-right.dak.3bcf415dCWGUBj#/manage/ak)来获取。<br/>建议您使用环境变量的方式调用用户名和密码，降低密码泄露风险。 |
| password | 无 | 是 | 当前账号的AccessKey Secret。您可以单击[AccessKey 管理](https://usercenter.console.aliyun.com/?spm=5176.2020520153.nav-right.dak.3bcf415dCWGUBj#/manage/ak)来获取。<br/>建议您使用环境变量的方式调用用户名和密码，降低密码泄露风险。 |
| table | 无 | 写入时必填,读取可以选择使用query参数,详见下方[读取参数](https://code.alibaba-inc.com/hologram/hologres-connectors/blob/master/hologres-connector-spark-3.x/README.md#%E8%AF%BB%E5%8F%96%E5%8F%82%E6%95%B0) | Hologres 读写数据的表名称 |
| jdbcurl | 无 | 是 | Hologres实时数据API的jdbcUrl，格式为`jdbc:postgresql://<host>:<port>/<db_name>`<br/> 您可以进入[Hologres管理控制台](https://hologram.console.aliyun.com/#/instance)的实例详情页，从实例配置获取主机和端口号<br/> |
| enable_serverless_computing | false | 否 | 是否使用serverless资源, 仅对读取和bulk_load写入有效,详见[serverless computing](https://help.aliyun.com/zh/hologres/user-guide/serverless-computing) |
| serverless_computing_query_priority | 3 | 否 | serverless computing执行优先级 |
| statement_timeout_seconds | 28800（8 小时） | 否 | 单位为 s，表示 query执行的超时时间 |
| retry_count | 3 | 否 | 当连接故障时的重试次数 |
| direct_connect | 对于可以直连的环境会默认使用直连 | 否 | 批量数据读写的瓶颈往往是endpoint的网络吞吐，因此我们会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。 |


### 写入参数
> connector 支持 Spark 的SaveMode 参数。对 SQL 来说，即`INSERT INTO` 或者 `INSERT OVERWRITE` 。对 DataFrame 来说，即 write 时设置 SaveMode 为 Append 或者 Overwrite 。其中Overwrite会创建临时表进行写入并在写入成功之后替换原始表，请在必要时使用。
>

| 参数名 | 参数曾用名 | 默认值 | 是否必填 | 说明 |
| :---: | --- | :---: | :---: | --- |
| write.mode | <font style="color:rgb(31, 35, 40);">copy_write_mode</font> | auto | 否 | 写入的模式，取值如下：<br/>1. auto（默认值），connector会根据版本和目标表的元信息自动选择最佳的模式，选择逻辑如下：<br/>    &nbsp;&nbsp;&nbsp;&nbsp;a. hologres 实例版本大于 2.2.25， 表有主键，选择bulk_load_on_conflict 模式<br/>    &nbsp;&nbsp;&nbsp;&nbsp;b. hologres 实例版本大于 2.1.0， 表无主键，选择bulk_load 模式<br/>    &nbsp;&nbsp;&nbsp;&nbsp;c. hologres 实例版本大于 1.3，选择 stream 模式<br/>    &nbsp;&nbsp;&nbsp;&nbsp;d. 其他情况，选择 insert 模式<br/>2. stream，即[fixed copy](https://help.aliyun.com/zh/hologres/user-guide/accelerate-the-execution-of-sql-statements-by-using-fixed-plans#section-i9l-6b7-bw6)。fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以有更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)。 注：需要connector版本>=1.3.0，hologres引擎版本>=r1.3.34<br/>3. bulk_load，即批量copy。批量copy相比流式的fixed copy，在rps相同时，holo实例的负载更低， 仅支持写入无主键表。 注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0<br/>4. bulk_load_on_conflict，批量copy写入有主键表时，支持处理主键重复的情况。Hologres主键表的批量数据导入默认会触发表锁，限制了多个连接同时进行并发写入的能力。当前 Connector 支持根据目标Hologres表的DistributionKey对数据进行重分布，使每个Spark的Task只负责写一个shard的数据，将原本的表锁降低至shard级别，实现并发写入，提升写入性能。由于每个连接只需要维护很少 shard 的数据，此优化也可以显著降低小文件的数量，降低Hologres 的内存使用。测试表明，对数据进行 Repartiiton 之后再并发写入，相比 Stream 模式写入，可以减少约67%的系统负载。   注：需要connector版本>=1.4.2，hologres引擎版本>=r2.2.25<br/>5. insert，使用普通的insert方式写入 |
| write.copy.max_buffer_size | <font style="color:rgb(31, 35, 40);">max_cell_buffer_size</font> | 52428800（50MB） | 否 | 使用copy模式写入时，本地 buffer 的最大长度，一般无需调整，在写入字段较大如超长字符串导致buffer溢出时调大。 |
| write.copy.dirty_data_check | copy_write_dirty_data_check | false | 否 | 是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行，会对写入性能造成一定影响，非排查环节不建议开启 |
| write.on_conflict_action | write_mode | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:   INSERT_OR_IGNORE 当主键冲突时，不写入   INSERT_OR_UPDATE 当主键冲突时，更新相应列   INSERT_OR_REPLACE 当主键冲突时，更新所有列 |
| 以下参数仅write.mode 为 insert 时生效。 | | | | |
| write.insert.dynamic_partition | dynamic_partition | false | 否 | copy_write_mode 为 insert 时生效。true 表示写入分区表父表时，自动创建不存在的分区 |
| write.insert.batch_size | write_batch_size | 512 | 否 | 每个写入线程的最大批次大小，   在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 |
| write.insert.batch_byte_size | write_batch_byte_size | 2097152（2 * 1024 * 1024） | 否 | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，   在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 |
| write.insert.max_interval_ms | write_max_interval_ms | 10000 | 否 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| write.insert.thread_size | write_thread_size | 1 | 否 | 写入并发线程数（每个并发占用1个数据库连接） |




### 读取参数
| 参数名 | 参数曾用名<br/>（1.5.0 及之前版本） | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: | :---: | --- |
| read.mode | bulk_read | auto | 否 | 写入的模式，取值如下：<br/>1. auto（默认值），connector会根据版本和目标表的元信息自动选择最佳的模式，选择逻辑如下：<br/>    &nbsp;&nbsp;&nbsp;&nbsp;a. 如果读取的字段中包含 jsonb 类型，选择 select 模式<br/>    &nbsp;&nbsp;&nbsp;&nbsp;b. 如果实例版本高于 3.0.24，选择bulk_read_compressed 模式<br/>    &nbsp;&nbsp;&nbsp;&nbsp;c. 其他情况，选择 bulk_read 模式<br/>2. bulk_read，使用 copy out 的方式以 arrow 格式读取数据，性能是 select 方式的 3-4 倍。暂不支持读取 hologres 中的 jsonb 类型。<br/>3. bulk_read_compressed, 使用 copy out 的方式读取压缩过的 arrow 格式数据，相比压缩前可以节省 45%左右的带宽。<br/>4. select，使用普通的 select 方式读取 |
| read.max_task_count | max_partition_count | 80 | 否 | 将读取的Hologres表分为多个分区，每个分区对应一个spark task。如果holo表的shardcount小于此参数,分区数量最多为shardcount。 |
| read.copy.max_buffer_size |  | 52428800（50MB） | 否 | 使用copy模式 读取 时，本地 buffer 的最大长度，在字段较大出现异常时调大。 |
| read.push_down_predicate | push_down_predicate | true | 否 | 是否进行谓词下推，例如查询时的一些过滤条件。<br/>&nbsp;&nbsp;&nbsp;&nbsp; 目前支持常见filter过滤条件的下推，以及列裁剪：<br/> |
| read.push_down_limit | push_down_limit | true | 否 | 是否进行 limit 下推 |
| read.select.batch_size | scan_batch_size | 256 | 否 | read_mode 设置为 select 时生效。读取Hologres时scan操作一次fetch的行数 |
| read.select.timeout_seconds | scan_timeout_seconds | 60 | 否 | read_mode 设置为 select 时生效。读取Hologres时scan操作的超时时间 |
| read.query | query | 无 | 否 | 使用传入的query去读取hologres, 此参数与table参数二者只能设置一个<br/>&nbsp;&nbsp;&nbsp;&nbsp; 需要注意的是，使用query读取时只能单task读取，使用table方式读取时，会根据holo表的shardcount分为多个 task 并发读取。同时，query 方式读取不支持谓词下推。<br/> |




# 数据类型映射
| Spark类型 | Hologres类型 |
| --- | --- |
| ShortType | SMALLINT |
| IntegerType | INT |
| LongType | BIGINT |
| StringType | TEXT |
| StringType | JSON |
| StringType | JSONB |
| DecimalType | NUMERIC(38, 18) |
| BooleanType | BOOL |
| DoubleType | DOUBLE PRECISION |
| FloatType | FLOAT |
| TimestampType | TIMESTAMPTZ |
| DateType | DATE |
| BinaryType | BYTEA |
| BinaryType | ROARINGBITMAP |
| ArrayType(IntegerType) | int4[] |
| ArrayType(LongType) | int8[] |
| ArrayType(FloatType | float4[] |
| ArrayType(DoubleType) | float8[] |
| ArrayType(BooleanType) | boolean[] |
| ArrayType(StringType) | text[] |




# 连接数计算
hologres spark connector在进行读写时，会使用一定的jdbc连接数。可能受到如下因素影响：

1. spark的并发，在作业运行时于spark UI处可以看到的同时运行的task数量
2. connector每个并发使用的连接数：copy方式写入，每个并发仅使用一个jdbc连接。insert 方式写入，每个并发会使用write_thread_size个jdbc连接。读取时每个并发使用一个jdbc连接。
3. 其他方面可能使用的连接数：作业启动时，会有schema获取等操作，可能短暂的建立1个连接

因此作业使用的总的连接数可以通过如下公示计算：

| 工作项 | 使用连接数 |
| --- | --- |
| catalog 查询元数据 | 1 |
| 读取数据 | parallelism * 1 + 1 |
| 写入copy模式 | parallelism * 1 + 1 |
| 写入insert模式 | parallelism * write_thread_size + 1 |


> spark 同时可以运行的task并发可能受到用户设置的参数影响，如`spark.executor.instances`，也可能受到hadoop对文件分块策略的影响，详情可以参考spark相关文档。以上连接数计算假设 spark 可同时运行的 task 数大于任务生成的 task 数。
>


