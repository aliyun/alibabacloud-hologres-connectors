# Spark-connector-Examples
在Examples模块下，有如下几个示例：

* 1.SparkDataFrameToHoloExample

  一个使用java实现的通过Holo Spark connector将数据写入至Hologres的应用
  使用scala脚本实现的例子可以参考 hologres-connector-spark-3.x/README.md

* 2.SparkHoloToDataFrameExample

  一个使用java实现的通过Holo Spark connector从Hologres读取数据的应用
  使用scala脚本实现的例子可以参考 hologres-connector-spark-3.x/README.md

* 3.SparkToHoloRepartitionExample

  一个使用scala实现的通过Holo Spark connector将数据根据holo的distribution key进行repartition，从而实现高性能的批量导入holo有主键表的应用

  
## 编译

在本项目(hologres-connector-spark-examples)根目录运行```mvn package -DskipTests```

### 运行Example 1

#### 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建结果表:

```create table sink_table(user_id bigint, user_name text, price decimal(38,12), sale_timestamp timestamptz);```

#### 提交Spark作业
当前的Spark example默认使用Spark 3.3版本，测试的时候请使用Spark 3.3版本集群

```
spark-submit --class com.alibaba.hologres.spark.example.SparkDataFrameToHoloExample --jars target/hologres-connector-spark-examples-1.0.0-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename sink_table
```

### 运行Example 2

#### 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建表并插入测试数据:

```
create table source_table(user_id bigint, user_name text, price decimal(38,12), sale_timestamp timestamptz);
insert into source_table select generate_series(1,20), 'abcd', 123.45, now();
```

#### 提交Spark作业
当前的Spark example默认使用Spark 3.3版本，测试的时候请使用Spark 3.x版本集群

```
spark-submit --class com.alibaba.hologres.spark.example.SparkHoloToDataFrameExample --jars target/hologres-connector-spark-examples-1.0.0-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename source_table
```

### 运行Example 3

#### 1. 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建分区表及分区子表:

```sql
CREATE TABLE test_table_batch
(
    C_CUSTKEY    BIGINT ,
    C_NAME       TEXT   ,
    C_ADDRESS    TEXT   ,
    C_NATIONKEY  INT    ,
    C_PHONE      TEXT   ,
    C_ACCTBAL    DECIMAL(15,2) ,
    C_MKTSEGMENT TEXT   ,
    C_COMMENT    TEXT   , 
    c_date date,
    primary key(C_CUSTKEY,c_date)
) PARTITION BY LIST(c_date);

CREATE TABLE test_table_batch_20240527 PARTITION OF test_table_batch
  FOR VALUES IN ('2024-05-27');
```
#### 2. 在resource/setting.properties 中配置连接、db以及数据源信息
测试数据源读取的是tpc-h测试使用到的customer.tbl文件,demo使用的数据源路径在resource/customer.tbl

#### 3. SparkToHoloRepartitionExample 代码核心逻辑
* Spark 通过内置的csv format读取tbl格式的文件为DataFrame
* 自定义分区策略,根据distribution_key对应字段计算出每条数据的shard,将数据重新分区为shardCount个partition
* spark会将这些partition的数据分发到不同的task上运行,每个task在运行时，只会写一个shard的数据,可以更好的内存中攒批,同时减少需要合并的小文件数量
* 当写入有主键表时,connector会根据当前所处的task number计算出当前task写入的shard,并设置target-shards参数,这样批量导入由默认的表锁改为shard级别的锁,不同的shard可以并发写入,提高写入性能

#### 4. SparkToHoloRepartitionExample 注意事项
* 本demo主要应用于批量导入数据
* 写入无主键表时,需要设置copy_write_mode和bulk_load参数为true
* 写入有主键表时,还需要设置enable_target_shards参数为true,要求批量导入之前有主键表是空表
* 参数说明:
  * copy_write_mode: 使用COPY模式写入,默认fixed_copy,fixed copy是hologres1.3新增的能力，相比insert方法，fixed copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)，但不支持回撤
  * bulk_load: 启用批量COPY导入，与jdbcCopyWriteMode参数同时设置为true时生效，批量copy相比fixed copy，写入时使用的hologres资源更小，默认情况下,仅支持写入无主键表,写入有主键表时会有表锁
  * enable_target_shards: bulkload写入有主键表时,默认是表锁.因此在上游数据根据shard进行了repartition的基础上,可以开启此参数.写入有主键表的锁粒度会从表级别调整为shard级别,相比fixedcopy写入有主键表，可以节省holo实例2/3的负载


#### 提交Spark作业

```
spark-submit --class com.alibaba.hologres.spark.example.SparkHoloToDataFrameExample --jars target/hologres-connector-spark-examples-1.0.0-jar-with-dependencies.jar
```
## 在IDEA中运行和调试
以上是针对提交作业到Spark集群的情况，用户也可以在IDEA等编辑器中运行代码，需要在运行配置中设置"将带有provided作用域的依赖项添加到类路径"