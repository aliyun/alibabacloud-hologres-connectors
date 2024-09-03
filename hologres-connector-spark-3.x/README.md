## 依赖hologres-connector-spark-base，实现了Spark 3.x版本的Connector
- 支持spark-shell、spark-sql、pyspark


## 准备工作

- 需要**Hologres 0.9**及以上版本。
- 需要**spark3.x**以及**scala2.12.x**

### 从中央仓库获取jar

可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。

```xml

<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-spark-3.x</artifactId>
    <version>1.4.1</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```

### 自行编译

connector依赖父项目的pom文件，在本项目根目录执行以下命令进行install

```
mvn clean install -N
```

#### build base jar 并 install 到本地maven仓库

- -P指定相关版本参数，本项目使用scala2.12以及spark3.0，详情请查看hologres-connector-spark-base子项目README


  ```
  mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.12 -Pspark-3
  ```

打包结果名称为 hologres-connector-spark-3.x-1.4.2-SNAPSHOT-jar-with-dependencies.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-3.x clean package -DskipTests
  ```

## 注意事项

### 连接数使用
hologres spark connector在进行读写时，会使用一定的jdbc连接数。可能受到如下因素影响：

1. spark的并发，在作业运行时于spark UI处可以看到的同步执行的task数量
2. connector每个并发使用的连接数：fixed copy方式写入，每个并发仅使用一个jdbc连接。insert
   方式写入，每个并发会使用write_thread_size个jdbc连接。读取时每个并发使用一个jdbc连接。
3. 其他方面可能使用的连接数：作业启动时，会有schema获取等操作，可能短暂的建立1个连接

因此作业使用的总的连接数可以通过如下公示计算：

* copy 模式： parallelism * 1 + 1
* 普通insert模式： parallelism * write_thread_size + 1

> spark task并发可能受到用户设置的参数影响，也可能受到hadoop对文件分块策略的影响，详情可以参考spark相关文档。

### SaveMode 
- Append: hologres-connector1.4.2版本之前，只支持Append类型的SaveMode。
- Overwrite: hologres-connector1.4.2版本开始，支持设置SaveMode为Overwrite类型，会创建临时表进行写入并在写入成功之后替换原始表，请谨慎使用。1.4.2版本Overwrite仅支持写public schema下的普通表。
  hologres-connector1.4.1版本开始，OverWrite支持写入带schema的普通表和分区子表(要求写入的子表已经存在),不支持写入分区父表,建议升级到此版本再使用Overwrite。

## 使用示例-批量导入

### 1.手动创建Hologres表并组织数据进行写入

#### 1.1 创建holo表

```sql
CREATE TABLE tb008 (
  id BIGINT primary key,
  counts INT,
  name TEXT,
  price NUMERIC(38, 18),
  out_of_stock BOOL,
  weight DOUBLE PRECISION,
  thick FLOAT,
  time TIMESTAMPTZ,
  dt DATE, 
  by bytea,
  inta int4[],
  longa int8[],
  floata float4[],
  doublea float8[],
  boola boolean[],
  stringa text[]
);
```

#### 1.2 组织数据并存入Holo

- 可以 spark-shell --jars hologres-connector-spark-3.x-1.4.2-SNAPSHOT-jar-with-dependencies.jar，然后spark-shell里执行测试
- 可以使用 :load spark-test.scala 执行测试文件
- spark-test.scala 文件示例：

```scala
import java.sql.{Timestamp, Date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
val intArray = Array(1, 2, 3)
val longArray = Array(1L, 2L, 3L)
val floatArray = Array(1.2F, 2.44F, 3.77F)
val doubleArray = Array(1.222, 2.333, 3.444)
val booleanArray = Array(true, false, false)
val stringArray = Array(null, "bcde", "defg") //hologres不支持数组元素为null，null将在holo中写为空字符串""

val data = Seq(
  Row(-7L, 100, "phone1", BigDecimal(1234.567891234), false, 199.35, 6.7F, Timestamp.valueOf("2021-01-01 00:00:00"), Date.valueOf("2021-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
  Row(6L, -10, "phone2", BigDecimal(1234.56), true, 188.45, 7.8F, Timestamp.valueOf("2021-01-01 00:00:00"), Date.valueOf("1970-01-01"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray),
  Row(1L, 10, "phone3\"", BigDecimal(1234.56), true, 111.45, null, Timestamp.valueOf("2020-02-29 00:12:33"), Date.valueOf("2020-07-23"), byteArray, intArray, longArray, floatArray, doubleArray, booleanArray, stringArray)
)


val schema = StructType(Array(
  StructField("id", LongType),
  StructField("counts", IntegerType),
  StructField("name", StringType, false), //false表示此Field不允许为null
  StructField("price", DecimalType(38, 12)),
  StructField("out_of_stock", BooleanType),
  StructField("weight", DoubleType),
  StructField("thick", FloatType),
  StructField("time", TimestampType),
  StructField("dt", DateType),
  StructField("by", BinaryType),
  StructField("inta", ArrayType(IntegerType)),
  StructField("longa", ArrayType(LongType)),
  StructField("floata", ArrayType(FloatType)),
  StructField("doublea", ArrayType(DoubleType)),
  StructField("boola", ArrayType(BooleanType)),
  StructField("stringa", ArrayType(StringType))
))


val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
df.show()

//配置导入数据至Hologres的信息。
df.write.format("hologres") //必须配置为hologres
.option("username", "your_username") //阿里云账号的AccessKey ID。
.option("password", "your_password") //阿里云账号的Accesskey SECRET。
.option("endpoint", "hologres_endpoint") //Hologres实时数据API的endpoint。
.option("database", "test_database") //Hologres的数据库名称,示例为test_database。
.option("table", "tb008") //Hologres用于接收数据的表名称，示例为tb008。
.option("write_mode", "insert_or_update") //写入Holo的类型，具体见下方参数介绍
.mode(SaveMode.Overwrite) // spark DataFrameWriter接口的SaveMode, 必须为Append；注意与WRITE_MODE不是同一个参数, 自hologres-connector1.3.3版本开始，支持SaveMode.OverWrite，会清理原始表中的数据，请谨慎使用
.save()
```

其中

```scala
.option("endpoint", "hologres_endpoint") //Hologres实时数据API的endpoint。
.option("database", "test_database") //Hologres的数据库名称,示例为test_database。
```

可以替换为（可选）

```scala
.option("jdbcurl", "jdbc:postgresql://hologres_endpoint/test_database") //Hologres实时数据API的jdbcUrl,与endpoint+database的设置二选一
```

### 2. 使用Spark shell从其他数据源读取数据并存入Holo

- 以Hive、postgressql为例，也可以是spark支持的其他数据源（如parquet格式的文件等）

  使用Spark从Hive中读取数据

```scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

val sparkConf = new SparkConf()
val sc = new SparkContext(sparkConf)
val hiveContext = new HiveContext(sc)

// Read from some table, for example: phone
val readDf = hiveContext.sql("select * from hive_database.phone")
```

使用spark从postgres/hologres中读取数据

```scala
// Read from some table, for example: tb008
val readDf = spark.read
.format("jdbc") //
.option("url", "jdbc:postgresql://Ip:Por/test_database")
.option("dbtable", "tb008")
.option("user", "your_username")
.option("password", "your_password")
.load()
```

将读取的数据写入hologres

```scala
import org.apache.spark.sql.SaveMode

// 函数实现见测试用例,也可以手动创建数据表
val table = createTableSql(readDf.schema, "tb009")

val df = spark.createDataFrame(
  readDf.rdd,
  readDf.schema
)

// Write to hologres table, for example: tb009
df.write
.format("hologres")
.option("username", "your_username")
.option("password", "your_password")
.option("endpoint", "hologres_endpoint")
.option("database", "test_database")
.option("table", table)
.mode(SaveMode.Append) // spark DataFrameWriter接口的SaveMode, 必须为Append；注意与WRITE_MODE不是同一个参数，自hologres-connector1.3.3版本开始，支持SaveMode.OverWrite，会清理原始表中的数据，请谨慎使用
.save()
```

### 使用pyspark加载connector进行写入

启动pyspark并加载connector
```shell
pyspark --jars hologres-connector-spark-3.x-1.4.2-SNAPSHOT-jar-with-dependencies.jar
```

与spark-shell类似，使用源数据创建DataFrame之后调用connector进行写入
```python
data = [[1, "Elia"], [2, "Teo"], [3, "Fang"]]
df = spark.createDataFrame(data, schema="id LONG, name STRING")
df.show()

df2.write.format("hologres").option(
  "username", "your_username").option(
  "password", "your_password").option(
  "endpoint", "hologres_endpoint").option(
  "database", "test_database").option(
  "table", "tb008").mode("append").save()
```

### 使用Spark Sql加载connector进行写入

启动 spark-sql 并加载 connector
```shell
spark-sql --jars hologres-connector-spark-3.x-1.4.2-SNAPSHOT-jar-with-dependencies.jar
```

通过spark sql DDL，分别创建csv 和 hologres view， 进行写入。
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
    path "resources/customer1.tbl", sep "|"
);

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
    jdbcurl "jdbc:postgresql://hologres_endpoint/test_database",
    username "your_username", 
    password "your_password", 
    table "customer_holo_table", 
    copy_write_mode "true", 
    bulk_load "true", 
    copy_write_format "text"
);

-- 目前通过sql创建的hologres view不支持写入部分列（如insert into hologresTable(c_custkey) select c_custkey from csvTable），写入时需要写入DDL中声明的所有字段。如果希望写入部分列，可以建表时仅声明需要写入的字段。
insert into hologresTable select * from csvTable;
```

## 参数说明

|             参数名             |                         默认值                         |           是否必填            |                                                                                                                                                      说明                                                                                                                                                       |
|:---------------------------:|:---------------------------------------------------:|:-------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|          username           |                          无                          |             是             |                                                                                                                                              阿里云账号的AccessKey ID                                                                                                                                               |
|          password           |                          无                          |             是             |                                                                                                                                            阿里云账号的Accesskey SECRET                                                                                                                                             |
|            table            |                          无                          |             是             |                                                                                                                                              Hologres用于接收数据的表名称                                                                                                                                               |
|          endpoint           |                          无                          |        与jdbcUrl二选一        |                                                                                                                                            Hologres实时数据API的Ip和Port                                                                                                                                            |
|          database           |                          无                          |        与jdbcUrl二选一        |                                                                                                                                             Hologres接收数据的表所在数据库名称                                                                                                                                             |
|           jdbcurl           |                          无                          | 与endpoint+database组合设置二选一 |                                                                                                                                            Hologres实时数据API的jdbcUrl                                                                                                                                            |
|       copy_write_mode       |                        true                         |             否             |                                                               是否使用copy方式写入，默认使用fixed copy。 <br> fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批) <br> 注：需要connector版本>=1.3.0，hologres引擎版本>=r1.3.34                                                                |
|      copy_write_format      |                       binary                        |             否             |                                                                                                                                           底层是否走二进制协议，二进制会更快，否则为文本模式                                                                                                                                           |
|      bulk_load      | false  <br> 当Hologres实例版本大于等于2.1且写入的表是无主键表时，默认为true |             否             |             是否采用批量copy方式写入（与fixed copy不同，fixed copy是流式的, rps 相同时，bulkload模式比fixedcopy可以降低holo实例2/3的负载） <br> 推荐Hologres2.1版本且写入无主键表时使用此参数。Hologres2.1优化了无主键表写入能力，无主键表批量写入不产生表锁，改为行锁，可以与Fixed Plan同时进行。  <br> 注：需要connector版本>=1.4.2，hologres引擎版本>=r2.1.0 <br> spark3.x的bulkload支持写入无主键空表，详见3.x的文档              |
|      enable_target_shards      |                       false                         |             否             | 使用bulkload模式写入有主键表时，必须开启此参数。且写入的数据已经经过repartition,相关demo见[hologres-connector-spark-examples](hologres-connector-examples/hologres-connector-spark-examples/src/main/java/com/alibaba/hologres/spark/example/SparkToHoloRepartitionExample.scala)的示例SparkToHoloRepartitionExample  <br>建议写入空的有主键表以避免去重，达到最佳性能  <br> 注：需要connector版本>=1.4.1，hologres引擎版本>=r2.1.0  |
|      max_cell_buffer_size      |                   20971520（20MB）                    |             否             |                                                                                                                                             使用copy模式写入时，单个字段的最大长度                                                                                                                                             |
| copy_write_dirty_data_check |                        false                        |             否             |                                                                                                                   是否进行脏数据校验，打开之后如果有脏数据，可以定位到写入失败的具体行，RecordChecker会对写入性能造成一定影响，非排查环节不建议开启.                                                                                                                    |
|  copy_write_direct_connect  |                  对于可以直连的环境会默认使用直连                   |             否             |                                                                                                               copy的瓶颈往往是VIP endpoint的网络吞吐，因此我们会测试当前环境能否直连holo fe，支持的话默认使用直连。此参数设置为false则不进行直连。                                                                                                                |
|         write_mode          |                  INSERT_OR_REPLACE                  |             否             |                                                                                          当INSERT目标表为有主键的表时采用不同策略:<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE 当主键冲突时，更新所有列                                                                                           |
|      write_batch_size       |                         512                         |             否             |                                                                                                                       每个写入线程的最大批次大小，<br>在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交                                                                                                                        |
|    write_batch_byte_size    |              2097152（2 * 1024 * 1024）               |             否             |                                                                                                          每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交                                                                                                           |
|   use_legacy_put_handler    |                        false                        |             否             |                                                                       true时，写入sql格式为insert into xxx(c0,c1,...) values (?,?,...),... on conflict; false时优先使用sql格式为insert into xxx(c0,c1,...) select unnest(?),unnest(?),... on conflict                                                                        |
|    write_max_interval_ms    |                        10000                        |             否             |                                                                                                                                      距离上次提交超过writeMaxIntervalMs会触发一次批量提交                                                                                                                                      |
|     write_fail_strategy     |                   TYR_ONE_BY_ONE                    |             否             |                                                                                                       当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常                                                                                                       |
|      write_thread_size      |                          1                          |             否             |                                                                                                                                            写入并发线程数（每个并发占用1个数据库连接）                                                                                                                                             |
|         retry_count         |                          3                          |             否             |                                                                                                                                               当连接故障时，写入和查询的重试次数                                                                                                                                               |
|     retry_sleep_init_ms     |                        1000                         |             否             |                                                                                                                               每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs                                                                                                                               |
|     retry_sleep_step_ms     |                        10000                        |             否             |                                                                                                                               每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs                                                                                                                               |
|   connection_max_idle_ms    |                        60000                        |             否             |                                                                                                                                       写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放                                                                                                                                        |
|      dynamic_partition      |                        false                        |             否             |                                                                                                                                         若为true，写入分区表父表时，当分区不存在时自动创建分区                                                                                                                                         |
|    fixed_connection_mode    |                        false                        |             否             |                                                                                                             非copy write 模式（insert默认）下，写入和点查不占用连接数（beta功能，需要connector版本>=1.2.0，hologres引擎版本>=1.3）                                                                                                              |
|       scan_batch_size       |                         256                         |             否             |                                                                                                                                          读取Hologres时Scan操作一次fetch的行数                                                                                                                                          |
|    scan_timeout_seconds     |                         60                          |             否             |                                                                                                                                            读取Hologres时scan操作的超时时间                                                                                                                                             |
|      scan_parallelism       |                         10                          |             否             |                                                                                                                                     读取Hologres时的默认并发数，最大为holo表的shardcount                                                                                                                                     |

## 类型映射

|         spark          |       holo       |
|:----------------------:|:----------------:|
|       ShortType        |     SMALLINT     |
|      IntegerType       |       INT        |
|        LongType        |      BIGINT      |
|       StringType       |       TEXT       |
|       StringType       |       JSON       |
|       StringType       |      JSONB       |
|      DecimalType       | NUMERIC(38, 18)  |
|      BooleanType       |       BOOL       |
|       DoubleType       | DOUBLE PRECISION |
|       FloatType        |      FLOAT       |
|     TimestampType      |   TIMESTAMPTZ    |
|        DateType        |       DATE       |
|       BinaryType       |      BYTEA       |
|       BinaryType       |  ROARINGBITMAP   |
| ArrayType(IntegerType) |      int4[]      |
|  ArrayType(LongType)   |      int8[]      |
|  ArrayType(FloatType   |     float4[]     |
| ArrayType(DoubleType)  |     float8[]     |
| ArrayType(BooleanType) |    boolean[]     |
| ArrayType(StringType)  |      text[]      |
