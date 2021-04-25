
## Hologres Flink Connector的通用核心代码

### Hologres Flink Connector相关参数说明

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


### hologres-connector-flink-base编译

使用-p参数指定flink版本进行编译，如flink1.11可以使用如下命令：

```
mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.11
```

