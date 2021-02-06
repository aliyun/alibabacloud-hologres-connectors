# 准备工作
- 永久
将hive-output-format-2.x-1.0-SNAPSHOT-jar-with-dependencies.jar放在HiveServer2所在节点的$HIVE_HOME/auxlib目录下（目录不存在就新建）
- session级
1. 将hive-output-format-2.x-1.0-SNAPSHOT-jar-with-dependencies.jar上传至hdfs
2. 在hive session中，使用add jar引入jar包。add jar hdfs:....

# 使用方式
1. 创建holo表
```sql
create table hive_ttqs(
    id int not null,
    amount numeric(12,3),
    ts timestamptz
)
```
2. 创建hive外表 外表字段类型一定要和holo保持一致！！！！！
```sql
CREATE EXTERNAL TABLE test0
(
  id int,
  amount decimal(12,3),
  ts timestamp
)
STORED BY 'com.alibaba.hologres.hive.HoloStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://host:port/db?reWriteBatchedInserts=true",
    "hive.sql.username" = "",
    "hive.sql.password" = "",
    "hive.sql.table" = "ttqs_hive"
);
```
3. insert into test0 select ...    


# 类型映射
|hive|holo|
|:-:|:-:|
|int|int|
|bigint|int8|
|boolean|bool|
|float|float|
|double|float|
|string|text|
|timestamp|timestamptz|
|date|date|
|decimal|numeric|