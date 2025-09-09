# Flink-connector-Examples

在Examples模块下，有如下几个示例：

* 1.FlinkSQLToHoloExample

  一个使用纯Flink SQL接口实现的应用，将数据写入至Hologres
* 2.FlinkDSAndSQLToHoloExample

  一个混合Flink DataStream 以及SQL 接口实现的应用，写入Hologres前，将DataStream转换成Table，之后再用Flink SQL写入Hologres
* 3.FlinkDataStreamToHoloExample

  一个使用纯Flink DataStream接口实现的应用，将数据写入至Hologres
  > 在DataStream作业中，用户使用自定义的record类型，因此默认不支持delete消息。如果需要支持消息的回撤，需要在实现RecordConverter时对convert结果设置MutationType，详见此示例代码。
  同时需要connector的1.3.2及以上版本。
* 4.FlinkSQLSourceAndSinkExample

  一个使用纯Flink SQL接口实现的应用，从holo源表读取数据并写入holo结果表
* 5.FlinkRoaringBitmapAggJob

  一个使用FLink及RoaringBitmap，结合Hologres维表，实现实时去重统计UV的应用，并将统计结果写入Hologres
* 6.FlinkToHoloRePartitionExample

  一个使用FLink DataStream接口实现的应用，将数据根据holo的shard进行repartition之后再写入至Hologres，可以减少hologres实例侧的小文件数量，提高写入性能的情况下降低负载。

## 在IDEA中运行和调试

*
以下是针对提交作业到Flink集群的情况，用户也可以在IDEA等编辑器中运行代码，只需要将pom.xml文件中各flink依赖的`<scope>provided</scope>`
删除即可

## Flink 1.15

* 当前example使用Flink 1.15版本, 如需使用其他版本的Flink, 需要适当调整将pom.xml文件中flink依赖项的版本

## 运行Example 1,2,3

### 编译

在本项目(hologres-connector-flink-examples)根目录运行```mvn package -DskipTests```

### 创建Hologres结果表用于接收数据

在自己的Hologres实例，创建结果表:

```create table sink_table(user_id bigint, user_name text, price decimal(38,2), sale_timestamp timestamptz);```

### 提交Flink Example作业

当前example默认使用Flink 1.15版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkDSAndSQLToHoloExample target/hologres-connector-flink-examples-1.3-SNAPSHOT-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename sink_table
```

其中需要替换对应的endpoint、username、password、database参数

## 运行Example 4

### 编译

在本项目(hologres-connector-flink-examples)根目录运行```mvn package -DskipTests```

### 创建Hologres源表和结果表

在自己的Hologres实例，创建源表并插入数据，并创建结果表:

```
create table source_table(user_id bigint, user_name text, price decimal(38,2),sale_timestamp timestamptz);
insert into source_table values(123,'Adam',123.11,'2022-05-19 14:33:05.418+08');
insert into source_table values(456,'Bob',123.456,'2022-05-19 14:33:05.418+08');
create table sink_table(user_id bigint, user_name text, price decimal(38,2), sale_timestamp timestamptz);
```

### 提交Flink Example作业

当前example默认使用Flink 1.15版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkSQLSourceAndSinkExample target/hologres-connector-flink-examples-1.3-SNAPSHOT-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --source source_table --sink sink_table
```

其中需要替换对应的endpoint、username、password、database参数

## 运行Example 5

### 使用Flink+RoaringBitmap进行实时用户标签去重(UV)

### 案例介绍

* 每天有几亿条数据，客户总量千万级，根据十个左右维度（支持维度间任意组合）查询相应的用户数去重统计信息，得出用户数精确去重指标，基于此设计基础聚合表。

* 主体思想：
    * 通过Flink+RoaringBitmap，把输入数据根据查询维度聚合出的uid结果放入roaringbitmap中，把roaringbitmap和查询维度存放在聚合结果表
    * 查询时，直接按照查询条件查询聚合的结果表，并对其中关键的roaringbitmap字段做or运算后并统计基数，即可得出对应用户数。

### 1.创建相关基础表

* 表uid_mapping为uid映射表。uid映射表用于映射uid到32位int类型。

    *
    roaringbitmap类型要求用户ID必须是32位int类型且越稠密越好（就是用户ID最好连续），而常见的业务系统或者埋点中的用户ID很多是字符串类型或Long类型，因此使用uid_mapping类型构建一张映射表。映射表利用Hologres的SERIAL类型（自增的32位int）来实现用户映射的自动管理和稳定映射。由于是实时数据,
    设置该表为行存表，以提高Flink维表实时join的QPS。

```sql
BEGIN;
CREATE TABLE public.uid_mapping (
uid text NOT NULL,
uid_int32 serial,
PRIMARY KEY (uid)
);
--将uid设为clustering_key和distribution_key便于快速查找其对应的int32值
CALL set_table_property('public.uid_mapping', 'clustering_key', 'uid');
CALL set_table_property('public.uid_mapping', 'distribution_key', 'uid');
CALL set_table_property('public.uid_mapping', 'orientation', 'row');
COMMIT;
```

* 表dws_app为基础聚合表。用于存放在基础维度上聚合后的结果。
    * 使用roaringbitmap前需要创建roaringbitmap extention，需要**Hologres 0.10**及以上版本

```
CREATE EXTENSION IF NOT EXISTS roaringbitmap_hqe;

```

* 创建结果表

```
--为了更好性能，建议根据基础聚合表数据量设置shard数；
--可以将基础聚合表的shard数设置不超过计算资源的core数；
--推荐用以下方式设置table group来设置shard
BEGIN;
CREATE TABLE tg16 (a int);                             --table group哨兵表
--新建shard数=16的table group
--因为测试数据量百万级，其中后端计算资源为100core，设置shard数为16
--shard数不建议设置超过core数
call set_table_property('tg16', 'shard_count', '16'); 
COMMIT;

begin;
create table dws_app(
  country text,
  prov text,
  city text, 
  ymd text NOT NULL,  --日期字段
  event_window_time TIMESTAMPTZ,  --统计时间戳，可以实现以Flink窗口周期为单位的统计
  uid32_bitmap roaringbitmap, -- 使用roaringbitmap记录uv
  primary key(country, prov, city, ymd, event_window_time)--查询维度和时间作为主键，防止重复插入数据
);
CALL set_table_property('public.dws_app', 'orientation', 'column');
--clustering_key和event_time_column设为日期字段，便于过滤
CALL set_table_property('public.dws_app', 'clustering_key', 'ymd');
CALL set_table_property('public.dws_app', 'event_time_column', 'ymd');
--等价于将表放在shard数=16的table group
call set_table_property('public.dws_app', 'colocate_with', 'tg16');
--distribution_key设为group by字段
CALL set_table_property('public.dws_app', 'distribution_key', 'country,prov,city');
end;
```

### 2.FLink实时读取数据并更新dws_app基础聚合表

#### 编译

```
cd hologres-connector-flink-examples
mvn package -DskipTests
```

#### 提交Flink 作业

当前example默认使用Flink 1.15版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkRoaringBitmapAggJob target/hologres-connector-flink-examples-1.5.6-SNAPSHOT-jar-with-dependencies.jar --propsFilePath src/main/resources/setting.properties --sourceFilePath ods_app_example.csv
```

需要在```src/main/resources/setting.properties```文件中替换对应的endpoint、username、password、database等参数,
并根据实际情况调整窗口大小

```properties
# hologres-flink-connector参数
endpoint=
username=
password=
database=
# 维表名
dimTableName=uid_mapping
# 结果表名
dwsTableName=dws_app
#窗口大小，单位为s
windowSize=2
```

程序中读取的csv格式示例在```src/main/resources/ods_app_example.csv```中,实际使用时结合数据源情况修改fieldsMask等

```csv
uid,country,prov,city,channel,operator,brand,ip,click_time,year,month,day,ymd
1ae58016,中国,广东,东莞,android,pros,a,192.168.1.1,2021-03-29 13:34:00,2021,3,29,20210329
2ae7788c,中国,广东,深圳,android,pros,a,192.168.1.8,2021-03-29 13:34:00,2021,3,29,20210329
1c90ab08,美国,新泽西州,阿布西肯,ios,cons,a,192.168.1.2,2021-03-29 13:34:00,2021,3,29,20210329
e7847f80,中国,天津,天津,ios,pros,b,192.168.1.3,2021-03-29 13:34:00,2021,3,29,20210329
1ae58016,中国,广东,东莞,android,pros,a,192.168.1.1,2021-03-29 13:34:00,2021,3,29,20210329
1c90ab08,美国,新泽西州,阿布西肯,ios,cons,a,192.168.1.2,2021-03-29 13:34:01,2021,3,29,20210329
e7847f80,中国,天津,天津,ios,pros,b,192.168.1.3,2021-03-29 13:34:01,2021,3,29,20210329
1ae58016,中国,广东,东莞,android,pros,a,192.168.1.1,2021-03-29 13:34:01,2021,3,29,20210329
1c90ab08,美国,新泽西州,阿布西肯,ios,cons,a,192.168.1.2,2021-03-29 13:34:01,2021,3,29,20210329
e7847f80,中国,天津,天津,ios,pros,b,192.168.1.3,2021-03-29 13:34:02,2021,3,29,20210329
1ae58016,中国,广东,东莞,android,pros,a,192.168.1.1,2021-03-29 13:34:02,2021,3,29,20210329
1c90ab08,美国,新泽西州,阿布西肯,ios,cons,a,192.168.1.2,2021-03-29 13:34:02,2021,3,29,20210329
e7847f80,中国,天津,天津,ios,pros,b,192.168.1.3,2021-03-29 13:34:02,2021,3,29,20210329
1ae58016,中国,广东,东莞,android,pros,a,192.168.1.1,2021-03-29 13:34:03,2021,3,29,20210329
1c90ab08,美国,新泽西州,阿布西肯,ios,cons,a,192.168.1.2,2021-03-29 13:34:03,2021,3,29,20210329
e7847f80,中国,天津,天津,ios,pros,b,192.168.1.3,2021-03-29 13:34:03,2021,3,29,20210329
e8855f8d,中国,天津,天津,ios,pros,b,192.168.1.5,2021-03-29 13:34:03,2021,3,29,20210329
```

#### FlinkRoaringBitmapAggJob 代码核心逻辑

* Flink 流式读取数据源（DataStream），并转化为源表（Table）
* 将源表与holo维表（id_mapping）进行关联
    * 其中维表使用insertifnotexists参数，查询不到时插入
* 关联结果转化为DataStream，通过Flink时间窗口处理，结合RoaringBitmap进行聚合，并以窗口为周期写入结果表。

### 3.查询

* resources/ods_app_example.csv中的示例数据，当窗口的大小为2s时，结果如下

```sql
select * from uid_mapping ;
   uid    | uid_int32 
----------+-----------
 1c90ab08 |         1
 2ae7788c |         9
 e7847f80 |         8
 e8855f8d |        10
 1ae58016 |         5

select country,prov,city,ymd,event_window_time, rb_to_array(uid32_bitmap) from dws_app order by event_window_time;
 country |   prov   |   city   |   ymd    |   event_window_time    | rb_to_array 
---------+----------+----------+----------+------------------------+-------------
 中国    | 广东     | 东莞     | 20210329 | 2021-03-29 13:34:02+08 | {5}
 中国    | 天津     | 天津     | 20210329 | 2021-03-29 13:34:02+08 | {8}
 美国    | 新泽西州  | 阿布西肯 | 20210329 | 2021-03-29 13:34:02+08 | {1}
 中国    | 广东     | 深圳     | 20210329 | 2021-03-29 13:34:02+08 | {9}
 美国    | 新泽西州  | 阿布西肯 | 20210329 | 2021-03-29 13:34:04+08 | {1}
 中国    | 广东     | 东莞     | 20210329 | 2021-03-29 13:34:04+08 | {5}
 中国    | 天津     | 天津     | 20210329 | 2021-03-29 13:34:04+08 | {8,10}
```

* 查询时，从基础聚合表(dws_app)中按照查询维度做聚合计算，查询bitmap基数，得出group by条件下的用户数

```sql
et hg_experimental_enable_force_three_stage_agg=off    
--运行下面RB_AGG运算查询，可先关闭三阶段聚合开关性能更佳（默认关闭）
```

* 查询某天内各个城市的uv

```
SELECT  country
        ,prov
        ,city
        ,RB_CARDINALITY(RB_OR_AGG(uid32_bitmap)) AS uv
FROM    dws_app
WHERE   ymd = '20210329'
GROUP BY country
         ,prov
         ,city
;
 country |   prov   |   city   | uv 
---------+----------+----------+----
 中国    | 广东     | 东莞     |  1
 美国    | 新泽西州  | 阿布西肯  |  1
 中国    | 广东     | 深圳     |  1
 中国    | 天津     | 天津     |  2
```

* 查询某段时间内各个省份的uv

```
SELECT  country
        ,prov
        ,RB_CARDINALITY(RB_OR_AGG(uid32_bitmap)) AS uv
FROM    dws_app
WHERE   event_window_time > '2021-03-29 13:30:00+08' and event_window_time <= '2021-03-29 13:34:05+08'
GROUP BY country
         ,prov
;

-- 结果如下
 country |   prov   | uv 
---------+----------+----
 美国    | 新泽西州  |  1
 中国    | 天津     |  2
 中国    | 广东     |  2
```

## 运行Example 6

### 使用Flink Custom Partition自定义数据分区策略,提高批量写入性能

### 1.创建hologres结果表

* 创建结果表

```
-- 建议根据数据量合理设置shard数
CREATE TABLE test_sink_customer
(
  c_custkey     BIGINT,
  c_name        TEXT,
  c_address     TEXT,
  c_nationkey   INT,
  c_phone       TEXT,
  c_acctbal     NUMERIC(15,2),
  c_mktsegment  TEXT,
  c_comment     TEXT,
  "date"        DATE
) with (
  distribution_key="c_custkey,date", 
  orientation="column"
);
```

### 2.运行作业

#### 编译

```
cd hologres-connector-flink-examples
mvn package -DskipTests
```

#### 提交Flink 作业

当前example默认使用Flink 1.15版本，实际使用时connector版本请与Flink集群的版本保持一致
-Dexecution.runtime-mode=BATCH 表示使用批模式提交作业,
详见[执行模式](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/execution_mode/)

```bash
# 使用以下命令提交作业, 通过sqlFilePath指定sql文件绝对路径,示例repartition.sql文件内容见下方,声明了sourceDDL、sourceDql、sinkDDL三条sql
flink run -Dexecution.runtime-mode=BATCH -c com.alibaba.ververica.connectors.hologres.example.FlinkToHoloRePartitionExample hologres-connector-flink-examples-1.5.6-SNAPSHOT-jar-with-dependencies.jar --sqlFilePath="xx/src/main/resources/repartition.sql"

# 或者分别指定sourceDDL,sourceDQL,sinkDDL三个参数,可能需要根据本地bash环境对换行符或者反引号等特殊字符进行转义
flink run -Dexecution.runtime-mode=BATCH -c com.alibaba.ververica.connectors.hologres.example.FlinkToHoloRePartitionExample hologres-connector-flink-examples-1.5.6-SNAPSHOT-jar-with-dependencies.jar --sourceDDL="create temporary table source_table (c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey INTEGER, c_phone STRING, c_acctbal NUMERIC(15, 2), c_mktsegment STRING, c_comment STRING) with ( 'connector' = 'datagen', 'rows-per-second' = '10000', 'number-of-rows' = '1000000' )" --sourceDQL="select *, cast('2024-04-21' as DATE) from source_table" --sinkDDL="create table sink_table (c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey INTEGER, c_phone STRING, c_acctbal NUMERIC(15, 2), c_mktsegment STRING, c_comment STRING, \`date\` STRING) with ( 'connector' = 'hologres', 'dbname' = 'test_db', 'tablename' = 'test_sink_customer', 'username' = '', 'password' = '', 'endpoint' = '', 'jdbccopywritemode' = 'true', 'bulkload' = 'true' )"
```

#### FlinkToHoloRePartitionExample 代码核心逻辑

* Flink SQL方式读取源表数据(Table),并将结果转化为DataStream<Row>
* 自定义分区策略,根据distribution_key对应字段计算出每条数据的shard,将数据重新分区到不同的并发上
* 每个并发上只会写入几个甚至一个shard的数据,可以更好的内存中攒批,同时减少需要合并的小文件数量
* 当写入有主键表时,程序根据当前并发将要写入的shard设置target-shards参数,将默认的表锁改为shard级别的锁,不同的shard可以并发写入,提高写入性能

#### FlinkToHoloRePartitionExample 注意事项

* 本demo主要应用于批量导入数据, 使用时建议使用Flink批模式提交作业,如果使用流模式,可以关闭自动checkpoint功能
* 写入无主键表时,需要设置jdbccopywritemode和bulkload参数为true
* 写入有主键表时,还需要设置reshuffle-by-holo-distribution-key.enabled参数为true,要求批量导入之前有主键表是空表
* flink结果表并发,建议与写入holo目标表的shard数一致

#### 参数说明

* sourceDDL: 源表声明, 示例中使用了datagen源表,使用时根据实际情况替换,注意运行环境中需要相关依赖
* sourceDQL: 源表查询, 查询结果会作为sinkDDL的输入, 因此要求select的字段数量,类型与sinkDDL声明的结果表对应
* sinkDDL: holo结果表声明, 因为需要通过Catalog获取连接信息,因此建表时必须使用CREATE TABLE方式(不能使用CREATE TEMPORARY
  TABLE),hologres connector会和demo代码一同打包,主要参数解释如下,其他参数的详细解释可以参考connector文档
    * jdbcCopyWriteMode:
        * 取值STREAM, 使用fixed copy写入, fixed copy是hologres1.3新增的能力，相比insert方法，fixed
          copy方式可以更高的吞吐（因为是流模式），更低的数据延时，更低的客户端内存消耗（因为不攒批)，但不支持回撤
        * 取值BULK_LOAD: 启用批量COPY导入，批量copy相比fixed copy，写入时使用的hologres资源更小，默认情况下,仅支持写入无主键表,写入有主键表时会有表锁
    * reshuffle-by-holo-distribution-key.enabled:
      bulkload写入有主键表时,默认是表锁.因此在上游数据根据shard进行了repartition的基础上,可以开启此参数.写入有主键表的锁粒度会从表级别调整为shard级别,相比fixedcopy写入有主键表，可以节省holo实例2/3的负载

```sql
--sourceDDL
CREATE TEMPORARY TABLE source_table
(
    c_custkey     BIGINT
    ,c_name       STRING
    ,c_address    STRING
    ,c_nationkey  INTEGER
    ,c_phone      STRING
    ,c_acctbal    NUMERIC(15, 2)
    ,c_mktsegment STRING
    ,c_comment    STRING
)
WITH (
    'connector' = 'datagen'
    ,'rows-per-second' = '10000'
    ,'number-of-rows' = '1000000'
);

--sourceDql
select *, cast('2024-04-21' as DATE) from source_table;

--sinkDDL
CREATE TABLE sink_table
(
    c_custkey     BIGINT
    ,c_name       STRING
    ,c_address    STRING
    ,c_nationkey  INTEGER
    ,c_phone      STRING
    ,c_acctbal    NUMERIC(15, 2)
    ,c_mktsegment STRING
    ,c_comment    STRING
    ,`date`       DATE
)
with (
    'connector' = 'hologres'
    ,'dbname' = 'test_db'
    ,'tablename' = 'test_sink_customer'
    ,'username' = ''
    ,'password' = ''
    ,'endpoint' = ''
    ,'jdbccopywritemode' = 'BULK_LOAD'
    ,'reshuffle-by-holo-distribution-key.enabled'='true'
);

```