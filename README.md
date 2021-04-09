# hologres-connectors
Connectors for Hologres

# 模块介绍
* hologres-connector-example 
  
    该模块提供了若干使用该项目下Connector的各种实例代码
* hologres-connector-flink-base 
  
    该模块实现了Hologres Flink Connector的通用核心代码
* hologres-connector-flink-1.11 
  
    依赖hologres-connector-flink-base，实现了Flink 1.11版本的Connector
* hologres-connector-flink-1.12 
  
    依赖hologres-connector-flink-base，实现了Flink 1.12版本的Connector，相较于1.11，主要新增了维表场景一对多的实现
* hologres-connector-hive-2.x 
  
    Hive的Hologres Connector
* hologres-connector-spark-base

    该模块实现了Hologres spark Connector的通用核心代码
* hologres-connector-spark-2.x

    依赖hologres-connector-spark-base，实现了spark2.x版本的Connector
* hologres-connector-spark-3.x 

    依赖hologres-connector-spark-base，实现了spark3.x版本的Connector

# 编译
在项目根目录执行
```mvn install -DskipTests``` 即可，各模块的maven依赖，可参考各自的pom.xml文件

# Hologres Flink Connector相关参数说明

| 参数 | 参数说明 | 是否必填 | 备注 |
| :--- | :--- | :--- | :--- |
| connector | 类型 | 是 | 值为：hologres |
| dbname | 读取的数据库 | 是 |  |
| tablename | 读取的表 | 是 |  |
| username | 用户名 | 是 |  |
| password | 密码 | 是 |  |
| mutatetype | 流式写入语义，见下面“流式语义”一节<br /> | 否 | 默认值：insertorignore |
| ignoredelete | 是否忽略撤回消息 | 否 | 默认值：false，只在流式语义下有效 |
| createparttable| 当写入分区表时，是否自动根据分区值自动创建分区表 | 否|默认值为false。建议慎用该功能，确保分区值不会出现脏数据导致创建错误的分区表。|
| connectionSize| 单个Flink Hologres Task所创建的JDBC连接池大小。|否|默认值为3，和吞吐成正比。|
| jdbcWriteBatchSize| Hologres Sink节点数据攒批的最大批大小。|否|默认值为256|
| jdbcWriteFlushInterval |Hologres Sink节点数据攒批的最长Flush等待时间）|否|默认值为10000，即10秒|

### Hologres Flink Connector 流式语义<br />
流式语义适用于持续不断往Hologres写入数据

- 流式语义做不做checkpointing，需要根据sink的配置和Hologres表的属性分为 exactly-once 或 at-least-once 语义
- 当Hologres表设有primary key时，Hologres sink可通过幂等性（idempotent）提供exactly-once语义
    - 在有primary key的情况下，出现同pk数据出现多次的情况时，Hologres sink支持以下流式语
        1. insertOrIgnore：保留首次出现的数据，忽略之后的所有数据。默认语义
        1. insertOrReplace：后出现的数据整行替换已有数据
        1. insertOrUpdate：部分替换已有数据。<br />a. 比如一张表有a,b,c,d四个字段，a是pk，然后写入的时候只写入a,b两个字段，在pk重复的情况下，会只更新b字段，c,d原有的值不变
> 说明：
> 1.hologres真实表含有主键的时候，实时写入默认会丢弃后来到来的数据来做去重(insertOrIngore)
> 2. 当mutatetype设置为insertOrUpdate或者insertOrReplace的时候会根据主键做更新

# Example
在Example模块下，有如下几个示例：

* FlinkSQLToHoloExample 一个使用纯Flink SQL接口实现的应用，将数据写入至Hologres
* FlinkDSAndSQLToHoloExample 一个混合Flink DataStream 以及SQL 接口实现的应用，写入Hologres前，将DataStream转换成Table，之后再用Flink SQL写入Hologres
* FlinkDataStreamToHoloExample 一个使用纯Flink DataStream接口实现的应用，将数据写入至Hologres

## 运行Example

### 编译
运行```mvn package -DskipTests```

### 创建Hologres结果表用于接收数据
在自己的Hologres实例，创建结果表:

```create table sink_table(user_id bigint, user_name text, price decimal(38,2), sale_timestamp timestamptz);```

### 提交Flink Example作业
当前的Flink example默认使用Flink 1.11版本进行编译，测试的时候请使用Flink 1.11版本实例

```
flink run -c com.alibaba.ververica.connectors.hologres.example.FlinkDSAndSQLToHoloExample hologres-connectors/hologres-connector-example/target/hologres-connector-example-1.0-SNAPSHOT-jar-with-dependencies.jar --endpoint ${ip:port} --username ${user_name} --password ${password} --database {database} --tablename sink_table
```

其中需要替换对应的endpoint、username、password、database参数