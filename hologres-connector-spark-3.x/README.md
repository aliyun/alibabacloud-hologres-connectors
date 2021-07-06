## 依赖hologres-connector-spark-base，实现了Spark 3.x版本的Connector

## 准备工作
- 需要**Hologres 0.9**及以上版本。
- 需要**spark3.x**以及**scala2.12.x**

#### build base jar 并 install 到本地maven仓库
  - -P指定相关版本参数，本项目使用scala2.12以及spark3.0，详情请查看hologres-connector-spark-base子项目README

  ```
  mvn install -pl hologres-connector-spark-base clean package -DskipTests -Pscala-2.12 -Pspark-3
  ```

  打包结果名称为 hologres-connector-spark-base_2.12_spark3-1.0-SNAPSHOT.jar

#### build jar

  ```
  mvn -pl hologres-connector-spark-3.x clean package -DskipTests
  ```

## 使用示例
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

- 可以 spark-shell --jars hologres-connector-spark-3.x-1.0-SNAPSHOT-jar-with-dependencies.jar，然后spark-shell里执行测试
- 可以使用 :load spark-test.scala 执行测试文件
- spark-test.scala 文件示例：

```scala
import java.sql.{Timestamp, Date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.alibaba.hologres.spark3.sink.SourceProvider

val byteArray = Array(1.toByte, 2.toByte, 3.toByte, 'b'.toByte, 'a'.toByte)
val intArray = Array(1, 2, 3)
val longArray = Array(1L, 2L, 3L)
val floatArray = Array(1.2F, 2.44F, 3.77F)
val doubleArray = Array(1.222, 2.333, 3.444)
val booleanArray = Array(true, false, false)
val stringArray = Array("abcd", "bcde", "defg")

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
  .option(SourceProvider.USERNAME, "your_username") //阿里云账号的AccessKey ID。
  .option(SourceProvider.PASSWORD, "your_password") //阿里云账号的Accesskey SECRET。
  .option(SourceProvider.ENDPOINT, "Ip:Port") //Hologres实时数据API的Ip和Port。
  .option(SourceProvider.DATABASE, "test_database") //Hologres的数据库名称,示例为test_database。
  .option(SourceProvider.TABLE, "tb008") //Hologres用于接收数据的表名称，示例为tb008。
  .option(SourceProvider.WRITE_MODE, "insert_or_update") //写入Holo的类型，具体见下方参数介绍
  .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, df.schema.toDDL) //输入DataFrame对应的DDL信息
  .mode(SaveMode.Append) // spark DataFrameWriter接口的SaveMode, 必须为Append；注意与WRITE_MODE不是同一个参数
  .save()
```

其中

```scala
.option(SourceProvider.endpoint, "Ip:Port")//Hologres实时数据API的Ip和Port。
.option(SourceProvider.database, "test_database")//Hologres的数据库名称,示例为test_database。
```

可以替换为（可选）

```scala
.option(SourceProvider.jdbcUrl, "jdbc:postgresql://Ip:Port/test_database")//Hologres实时数据API的jdbcUrl,与endpoint+database的设置二选一
```

### 2. 使用Spark sql从其他数据源读取数据并存入Holo
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
import com.alibaba.hologres.spark3.sink.SourceProvider

// 函数实现见测试用例,也可以手动创建数据表
val table = createTableSql(readDf.schema, "tb009")

val df = spark.createDataFrame(
  readDf.rdd,
  readDf.schema
)

// Write to hologres table, for example: tb009
df.write
  .format("hologres")
  .option(SourceProvider.USERNAME, "your_username")
  .option(SourceProvider.PASSWORD, "your_password")
  .option(SourceProvider.ENDPOINT, "Ip:Port")
  .option(SourceProvider.DATABASE, "test_database")
  .option(SourceProvider.TABLE, table)
  .option(SourceProvider.INPUT_DATA_SCHEMA_DDL, df.schema.toDDL)
  .mode(SaveMode.Append)
  .save()
```

## 参数说明

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| USERNAME | 无 | 是 | 阿里云账号的AccessKey ID |
| PASSWORD | 无 | 是 | 阿里云账号的Accesskey SECRET |
| TABLE | 无 | 是 | Hologres用于接收数据的表名称 |
| ENDPOINT | 无 | 与jdbcUrl二选一| Hologres实时数据API的Ip和Port |
| DATABASE | 无 | 与jdbcUrl二选一| Hologres接收数据的表所在数据库名称 |
| JDBCURL | 无 | 与endpoint+database组合设置二选一| Hologres实时数据API的jdbcUrl |
| INPUT_DATA_SCHEMA_DDL | 无 | 是 | 组成DataFrame的schema对应的DDL，<br> 使用"your_df.schema.toDDL"输入即可 |
| WRITE_MODE | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE 当主键冲突时，更新所有列|
| WRITE_BATCH_SIZE | 512 | 否 | 每个写入线程的最大批次大小，<br>在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 |
| WRITE_BATCH_BYTE_SIZE | 2097152（2 * 1024 * 1024） | 否 | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 |
| WRITE_MAX_INTERVAL_MS | 10000 | 否 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| WRITE_FAIL_STRATEGY | TYR_ONE_BY_ONE | 否 | 当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常 |
| WRITE_THREAD_SIZE | 1 | 否 | 写入并发线程数（每个并发占用1个数据库连接） |
| RETRY_COUNT | 3 | 否 | 当连接故障时，写入和查询的重试次数 |
| RETRY_SLEEP_INIT_MS | 1000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| RETRY_SLEEP_STEP_MS | 10000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs|
| CONNECTION_MAX_IDLE_MS| 60000 | 否 | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放|

## 类型映射
|spark|holo|
|:---:|:---:|
| IntegerType | INT |
| LongType | BIGINT |
| StringType | TEXT |
| DecimalType | NUMERIC(38, 18) |
| BooleanType | BOOL |
| DoubleType | DOUBLE PRECISION |
| FloatType | FLOAT |
| TimestampType | TIMESTAMPTZ |
| DateType | DATE |
| BinaryType | BYTEA |
| ArrayType(IntegerType) | int4[] |
| ArrayType(LongType) | int8[] |
| ArrayType(FloatType | float4[] |
| ArrayType(DoubleType) | float8[] |
| ArrayType(BooleanType) | boolean[] |
| ArrayType(StringType) | text[] |
