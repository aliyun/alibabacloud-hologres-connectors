# Flink-connector-Examples
在Examples模块下，有如下几个示例：

* 1.FlinkSQLToHoloExample
  
    一个使用纯Flink SQL接口实现的应用，将数据写入至Hologres
* 2.FlinkDSAndSQLToHoloExample
  
    一个混合Flink DataStream 以及SQL 接口实现的应用，写入Hologres前，将DataStream转换成Table，之后再用Flink SQL写入Hologres
* 3.FlinkDataStreamToHoloExample
  
    一个使用纯Flink DataStream接口实现的应用，将数据写入至Hologres
* 4.FlinkSQLSourceAndSinkExample

  一个使用纯Flink SQL接口实现的应用，从holo源表读取数据并写入holo结果表
* 5.FlinkRoaringBitmapAggJob

    一个使用FLink及RoaringBitmap，结合Hologres维表，实现实时去重统计UV的应用，并将统计结果写入Hologres

## 在IDEA中运行和调试
* 以下是针对提交作业到Flink集群的情况，用户也可以在IDEA等编辑器中运行代码，只需要将pom.xml文件中各flink依赖的`<scope>provided</scope>`删除即可

## Flink 1.14
* 当前example使用Flink 1.13版本, 如需使用Flink 1.14, 由于版本依赖变动，需要将pom.xml文件中各flink依赖的`-blink`后缀去掉

## 运行Example 1,2,3

### 编译

在本项目(hologres-connector-flink-examples)根目录运行```mvn package -DskipTests```

### 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建结果表:

```create table sink_table(user_id bigint, user_name text, price decimal(38,2), sale_timestamp timestamptz);```

### 提交Flink Example作业
当前example默认使用Flink 1.13版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkDSAndSQLToHoloExample target/hologres-connector-flink-examples-1.1.1-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename sink_table
```
其中需要替换对应的endpoint、username、password、database参数

## 运行Example 4

### 编译

在本项目(hologres-connector-flink-examples)根目录运行```mvn package -DskipTests```

### 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建结果表并插入数据，创建结果表:

```
create table source_table(user_id bigint, user_name text, price decimal(38,2),sale_timestamp timestamptz);
insert into source_table values(123,'Adam',123.11,'2022-05-19 14:33:05.418+08');
insert into source_table values(456,'Bob',123.456,'2022-05-19 14:33:05.418+08');
create table sink_table(user_id bigint, user_name text, price decimal(38,2), sale_timestamp timestamptz);
```

### 提交Flink Example作业
当前example默认使用Flink 1.13版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkSQLSourceAndSinkExample target/hologres-connector-flink-examples-1.1-SNAPSHOT-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --source source_table --sink sink_table
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

    * roaringbitmap类型要求用户ID必须是32位int类型且越稠密越好（就是用户ID最好连续），而常见的业务系统或者埋点中的用户ID很多是字符串类型或Long类型，因此使用uid_mapping类型构建一张映射表。映射表利用Hologres的SERIAL类型（自增的32位int）来实现用户映射的自动管理和稳定映射。由于是实时数据, 设置该表为行存表，以提高Flink维表实时join的QPS。

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
  timetz TIMESTAMPTZ,  --统计时间戳，可以实现以Flink窗口周期为单位的统计
  uid32_bitmap roaringbitmap, -- 使用roaringbitmap记录uv
  primary key(country, prov, city, ymd, timetz)--查询维度和时间作为主键，防止重复插入数据
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
cd hologres-connector-flink-example
mvn package -DskipTests
```

#### 提交Flink 作业
当前example默认使用Flink 1.13版本，实际使用时connector版本请与Flink集群的版本保持一致

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkRoaringBitmapAggJob target/hologres-connector-flink-examples-1.0.0-jar-with-dependencies.jar --propsFilePath src/main/resources/setting.properties --sourceFilePath ods_app_example.csv
```

其中需要在```src/main/resources/setting.properties```文件中替换对应的endpoint、username、password、database等参数
程序中读取的csv格式示例在```src/main/resources/ods_app_example.csv```中,实际使用时结合数据源情况修改fieldsMask等

#### FlinkRoaringBitmapAggJob 代码核心逻辑
* Flink 流式读取数据源（DataStream），并转化为源表（Table）
* 将源表与holo维表（id_mapping）进行关联
    * 其中维表使用insertifnotexists参数，查询不到时插入
* 关联结果转化为DataStream，通过Flink时间窗口处理，结合RoaringBitmap进行聚合，并以窗口为周期写入结果表。

### 3.查询

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
```

* 查询某段时间内各个省份的uv

```
SELECT  country
        ,prov
        ,RB_CARDINALITY(RB_OR_AGG(uid32_bitmap)) AS uv
FROM    dws_app
WHERE   time > '2021-04-19 18:00:00+08' and time < '2021-04-19 19:00:00+08'
GROUP BY country
         ,prov
;
```
