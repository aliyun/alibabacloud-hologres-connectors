## 实现了Kafka写入Hologres的Connector

## 准备工作
- 需要**Hologres 0.9**及以上版本
- 需要**Kafka-0.10.0.0**及以上版本
  - 建议使用Kafka-1.0.0及以上版本
  - kafka-0.10.0.0, kafka-0.10.1.0 不支持`whole_message_info=true`

### 从中央仓库获取jar
可以在项目pom文件中通过如下方式引入依赖，其中`<classifier>`必须加上，防止发生依赖冲突。
```xml
<dependency>
    <groupId>com.alibaba.hologres</groupId>
    <artifactId>hologres-connector-kafka</artifactId>
    <version>1.0.0</version>
    <classifier>jar-with-dependencies</classifier>
</dependency>
```
### 自行编译
#### build jar

  ```
  mvn -pl hologres-connector-kafka clean package -DskipTests
  ```

#### 加载jar包（以下两种方式可选，此处以放在工作目录为例）
* KAFKA_HOME：
    * 将hologres-connector-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar放在Kafka安装目录的libs中。即 `$KAFKA_HOME/libs`
* 工作目录
    * 将hologres-connector-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar放入你的工作目录，以下示例皆以`/user/xxx/workspace`为例
    * 在`connect-standalone.properties`中设置jar包路径，`plugin.path=/user/xxx/workspace`
    * kafka-1.0.0版本之前，不支持设置工作目录，只能将jar文件放入`$KAFKA_HOME/libs`中

#### 配置文件说明

配置文件在本项目的`hologres-connector-kafka/src/main/resources/*`路径中

* `holo-sink.properties`是hologres-connector-kafka的配置文件, 用于配置connector相关的参数，参数详情见下方的参数说明
* `connect-standalone.properties`是kafka原有的配置文件，原路径在kafka的安装目录的`$KAFKA_HOME/libexec/config`中，本项目将其复制一份方便使用

#### 消费模式

* 本 connector 目前含有如下三种消费模式，使用方式见下方示例
  * json: 接受只包含键值对的json`{key1:value1,key2:value2}`，其中key名称与hologres表字段名对应。
  * struct_json: 接受特定的json格式输入（包含`schema`以及`payload`的定义）,可以指定表schema，包括表的字段名、类型等，存入相应的hologres表中。
  * string: 接受`key:value`格式的输入，将key、value当作字符串存入特定的hologres表中，注意kafka的key，value通过tab键分割。

<br><br>

## 使用示例1：json模式
* json 模式消费的是只包含键值对的json`{key1:value1,key2:value2}`，其中key名称与hologres表字段名对应。

### 1.将jar包、配置文件复制到工作目录，本示例以单节点模式standalone为例:

```bash
cp hologres-connector-kafka/target/hologres-connector-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/connect-standalone.properties /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/holo-sink.properties /user/xxx/workspace/
```
### 2.修改配置文件

修改`holo-sink.properties`

```properties
input_format=json
connection.jdbcUrl=jdbc:postgresql://hostname:port/database_name
connection.username=your_username
connection.password=your_password
table=test_json
```

修改`connect-standalone.properties`

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
plugin.path=/user/xxx/workspace
```

### 3.创建Hologres表

```sql
create table test_json(
  id int PRIMARY KEY,
  name text,
  weight DOUBLE PRECISION,
  thick FLOAT,
  deci_0 numeric(15,3),
  time_0 timestamptz,
  kafkaTopic text,
  kafkaPartition int,
  kafkaOffset bigint,
  kafkaTimestamp timestamptz
);
```

### 4.开启producer并通过connector消费

#### 4.1 创建名称为`kafka_sink_test`的kafka topic:

```shell
kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka_sink_test
kafka-topics.sh --create --topic kafka_sink_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

#### 4.2 开启producer并输入数据

* 启动producer

```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic kafka_sink_test
```
* 复制下列json到producer terminal
  * 其中 timestamptz类型可以是时间戳（long）或者字符串 

```json
{"id":1,"name":"abcd","weight":123.456,"thick":12.34,"deci_0":456.789,"time_0":"2021-05-21 16:00:45"}
{"id":2,"name":"efgh","weight":123.456,"thick":12.34,"deci_0":456.789,"time_0":1567688965261}
```

#### 4.3 开启connector-cunstomer进行消费

```bash
connect-standalone.sh connect-standalone.properties holo-sink.properties
```

#### 4.4 写入hologres成功，数据如下图所示

```
 id | name | weight  | thick | deci_0  |           time_0           |   kafkatopic    | kafkapartition | kafkaoffset |       kafkatimestamp       
----+------+---------+-------+---------+----------------------------+-----------------+----------------+-------------+----------------------------
  2 | efgh | 123.456 | 12.34 | 456.789 | 2019-09-05 21:09:25.261+08 | kafka_sink_test |              0 |           0 | 2021-05-25 17:26:36.818+08
  1 | abcd | 123.456 | 12.34 | 456.789 | 2021-05-21 16:00:45+08     | kafka_sink_test |              2 |           2 | 2021-05-25 17:26:35.335+08
```

<br><br>

## 使用示例2：struct_json模式
* struct_json 模式消费的是特殊格式的json（包含schema以及payload的定义）,可以指定表的字段名、字段类型等，存入相应的hologres表中
 
### 1.将jar包、配置文件复制到工作目录，本示例以单节点模式standalone为例:

```bash
cp hologres-connector-kafka/target/hologres-connector-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/connect-standalone.properties /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/holo-sink.properties /user/xxx/workspace/
```
### 2.修改配置文件

修改`holo-sink.properties`

```properties
input_format=struct_json
connection.jdbcUrl=jdbc:postgresql://hostname:port/database_name
connection.username=your_username
connection.password=your_password
table=test_struct_json
```

修改`connect-standalone.properties`

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
plugin.path=/user/xxx/workspace
```

### 3.创建Hologres表

```sql
create table test_struct_json(
  id int PRIMARY KEY,
  name text,
  weight DOUBLE PRECISION,
  thick FLOAT,
  deci_0 numeric(15,3),
  time_0 timestamptz,
  deci_1 numeric(15,3),
  time_1 timestamptz,
  kafkaTopic text,
  kafkaPartition int,
  kafkaOffset bigint,
  kafkaTimestamp timestamptz
);
```

### 4.开启producer并通过connector消费

#### 4.1 创建名称为`kafka_sink_test`的kafka topic:

```shell
kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka_sink_test
kafka-topics.sh --create --topic kafka_sink_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

#### 4.2 开启producer并输入数据

* 启动producer

```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic kafka_sink_test
```
* 复制下列json到producer terminal

```json
{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"double","optional":false,"field":"weight"},{"type":"float","optional":false,"field":"thick"},{"name":"org.apache.kafka.connect.data.Decimal","type":"bytes","optional":false,"parameters":{"scale":"3","connect.decimal.precision":"15"},"field":"deci_0"},{"name":"org.apache.kafka.connect.data.Timestamp","type":"int64","optional":false,"field":"time_0"},{"name":"Decimal","type":"string","optional":false,"field":"deci_1"},{"name":"Timestamp","type":"string","optional":false,"field":"time_1"}],"optional":false,"name":"test_struct_json"},"payload":{"id":1,"name":"abcd","weight":123.456,"thick":12.34,"deci_0":"W42A","time_0":1567688965261,"deci_1":"999.8888","time_1":"2021-05-20 11:09:25"}}
```
* 以上是消费时被压缩为一行的json，格式化的完整json如下：
  * 可以看到，Decimal、Date、Timestamp三种类型需要通过`fields.name`声明，详见 4.5类型映射
  
```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": false,
        "field": "id"
      },
      {
        "type": "string",
        "optional": false,
        "field": "name"
      },
      {
        "type": "double",
        "optional": false,
        "field": "weight"
      },
      {
        "type": "float",
        "optional": false,
        "field": "thick"
      },
      {
        "name": "org.apache.kafka.connect.data.Decimal",
        "type": "bytes",
        "optional": false,
        "parameters": {
          "scale": "3",
          "connect.decimal.precision": "15"
        },
        "field": "deci_0"
      },
      {
        "name": "org.apache.kafka.connect.data.Timestamp",
        "type": "int64",
        "optional": false,
        "field": "time_0"
      },
      {
        "name": "Decimal",
        "type": "string",
        "optional": false,
        "field": "deci_1"
      },
      {
        "name": "Timestamp",
        "type": "string",
        "optional": false,
        "field": "time_1"
      }
    ],
    "optional": false,
    "name": "test_struct_json"
  },
  "payload": {
    "id": 1,
    "name": "abcd",
    "weight": 123.456,
    "thick": 12.34,
    "deci_0": "W42A7",
    "time_0": 1567688965261,
    "deci_1": "999.8888",
    "time_1": "2021-05-20 11:09:25"
  }
}
```
#### 4.3 开启connector-cunstomer进行消费

```bash
connect-standalone.sh connect-standalone.properties holo-sink.properties
```

#### 4.4 写入hologres成功，数据如下图所示

```
 id | name | weight  |      thick       |  deci_0  |           time_0           | deci_1  |         time_1         |   kafkatopic    | kafkapartition | kafkaoffset |       kafkatimestamp       
----+------+---------+------------------+----------+----------------------------+---------+------------------------+-----------------+----------------+-------------+----------------------------
  1 | abcd | 123.456 | 12.3400001525879 | 6000.000 | 2019-09-05 21:09:25.261+08 | 999.889 | 2021-05-20 11:09:25+08 | kafka_sink_test |              0 |           0 | 2021-05-24 13:40:41.264+08
```

#### 4.5 类型映射
* 仅针对input_format=struct_json，定义了相应字段的类型，可以进行类型映射

|kafka|holo|
|:---:|:---:|
|int8|INT|
|int16|INT|
|int32|INT|
|int64|BIGINT|
|boolean|BOOL|
|float|FLOAT|
|double|DOUBLE PRECISION|
|string|TEXT|
|Decimal|NUMERIC(38,18)|
|Date|DATE|
|Timestamp|TIMESTAMPTZ|

> 注1： Decimal、Date、Timestamp三种类型需要通过`fields.name`声明，声明方式分为两种，区别如下表。
> 前三行为org.apache.kafka.connect.data类型， 对输入格式要求较为严格，适合消费其他组件生成的struct_json数据时使用；
> 而直接消费文件等数据建议使用后三行的类型，即通过string写入，具有较高的可读性。


|name|需要的数据类型|对应的holo数据类型|
|:---:|:---:|:---:|
|org.apache.kafka.connect.data.Decimal|bytes|NUMERIC(38,18)|
|org.apache.kafka.connect.data.Date|long|DATE|
|org.apache.kafka.connect.data.Timestamp|long|TIMESTAMPTZ|
|Decimal|string|NUMERIC(38,18)|
|Date|string|DATE|
|Timestamp|string|TIMESTAMPTZ|

<br><br>

## 使用示例3：string模式
* string模式消费的是key:value格式的输入，将key、value当作字符串存入特定的hologres表中

### 1.将jar包、配置文件复制到工作目录，本示例以单节点模式standalone为例:

```bash
cp hologres-connector-kafka/target/hologres-connector-kafka-1.0-SNAPSHOT-jar-with-dependencies.jar /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/connect-standalone.properties /user/xxx/workspace/
cp hologres-connector-kafka/src/main/resources/holo-sink.properties /user/xxx/workspace/
```
### 2.修改配置文件

修改`holo-sink.properties`

```properties
input_format=string
connection.jdbcUrl=jdbc:postgresql://hostname:port/database_name
connection.username=your_username
connection.password=your_password
table=test_string
```

修改`connect-standalone.properties`

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
value.converter.schemas.enable=true
plugin.path=/user/xxx/workspace
```

### 3.创建Hologres表

```sql
create table test_string(
  key text,
  value text,
  kafkaTopic text,
  kafkaPartition int,
  kafkaOffset bigint,
  kafkaTimestamp timestamptz
);
```

### 4.开启producer并通过connector消费

#### 4.1 创建名称为`kafka_sink_test`的kafka topic:

```shell
kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka_sink_test
kafka-topics.sh --create --topic kafka_sink_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

#### 4.2 开启producer并输入数据

启动producer

```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic kafka_sink_test --property parse.key=true
```
复制下列数据到producer terminal,注意kafka消息的key:value通过tab键分割。

```bash
key1	value1
key2	value2
key3	value3
```

#### 4.3 开启connector-cunstomer进行消费

```bash
connect-standalone.sh connect-standalone.properties holo-sink.properties
```

#### 4.4 写入hologres成功，数据如下图所示

```
 key  | value  |   kafkatopic    | kafkapartition | kafkaoffset |       kafkatimestamp       
------+--------+-----------------+----------------+-------------+----------------------------
 key3 | value3 | kafka_sink_test |              0 |           2 | 2021-05-21 14:55:43.037+08
 key2 | value2 | kafka_sink_test |              0 |           1 | 2021-05-21 14:55:39.301+08
 key1 | value1 | kafka_sink_test |              0 |           0 | 2021-05-21 14:55:35.352+08
```

<br><br>


## 使用示例4：脏数据处理
* 基于使用示例1: json 模式进行展示

### 1.开启producer并通过connector消费

#### 1.1 创建名称为`kafka_sink_test`的kafka topic:

```shell
kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafka_sink_test
kafka-topics.sh --create --topic kafka_sink_test --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
```

#### 1.2 开启producer并输入数据

* 启动producer

```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic kafka_sink_test
```
* 复制下列json到producer terminal
  * 其中 timestamptz类型可以是时间戳（long）或者字符串

```json
{"id":1,"name":"abcd","weight":123.456,"thick":12.34,"deci_0":456.789,"time_0":"2021-05-21 16:00:45"}
{"id":2,"wrong_name":"efgh","weight":123.456,"thick":12.34,"deci_0":456.789,"time_0":"2021-05-21 16:00:45"}
```

#### 1.3 开启connector-cunstomer进行消费

```bash
connect-standalone.sh connect-standalone.properties holo-sink.properties
```

#### 1.4 写入hologres抛出异常

```
com.alibaba.hologres.kafka.exception.KafkaHoloException: 
If you want to skip this dirty data, please add < dirty_date_strategy=SKIP_ONCE > and < dirty_date_to_skip_once=kafka_sink_test,0,0 > in holo-sink.properties; or add < dirty_date_strategy=SKIP > to skip all dirty data(not recommended).

```

### 2.配置脏数据处理策略之后重试

#### 2.1 配置脏数据处理策略

```properties
dirty_date_strategy=SKIP_ONCE
dirty_date_to_skip_once=kafka_sink_test,0,0
```

#### 2.1 脏数据SKIP日志

```
[2021-09-02 20:00:56,586] WARN Skip(once) Dirty Data: SinkRecord{kafkaOffset=0, timestampType=CreateTime} ConnectRecord{topic='kafka_sink_test', kafkaPartition=0, key=null, keySchema=Schema{STRING}, value={time_0=2021-05-21 16:00:45, wrong_name=efgh, weight=123.456, deci_0=456.789, id=2, thick=12.34}, valueSchema=null, timestamp=1630583824222, headers=ConnectHeaders(headers=)} (com.alibaba.hologres.kafka.sink.HoloSinkWriter:85)
```

#### 2.3 其他数据写入成功

```
 id | name | weight  | thick | deci_0  |         time_0         |   kafkatopic    | kafkapartition | kafkaoffset |       kafkatimestamp       
----+------+---------+-------+---------+------------------------+-----------------+----------------+-------------+----------------------------
  1 | abcd | 123.456 | 12.34 | 456.789 | 2021-05-21 16:00:45+08 | kafka_sink_test |              1 |           0 | 2021-09-02 19:57:00.005+08
```

<br><br>

## kafka的两种connect模式
* kafka有两种connect模式，上述示例使用的是单节点模式，还有一种是分布式模式，其与单节点模式类似，相应的配置文件参数也相同
### 单节点模式

```bash
connect-standalone.sh connect-standalone.properties holo-sink.properties
```

### 分布式模式
分布式模式的`holo-sink.json`文件与单节点模式的`holo-sink.properties`参数相同，部分版本需要将`value.converter`等参数也在json中声明, <br>
`connect-distributed.properties`文件与单节点模式的`connect-standalone.properties`参数相同，详情请参考下方参数说明

```bash
connect-distributed.sh connect-distributed.properties
curl -s -X POST -H 'Content-Type: application/json' --data @holo-sink.json http://localhost:8083/connectors
```

<br><br>

## 参数说明

参数在配置文件`holo-sink.properties` 以及 `holo-sink.json`中指定

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| name | 无 | 是 | 此次运行的connector的名称 |
| connector.class | 无 | 是 | 必须为`com.alibaba.hologres.kafka.HoloSinkConnector` |
| tasks.max | 无 | 是 | 创建的最大任务数 |
| driver.class | 无 | 是 | 必须为`org.postgresql.Driver` |
| topics | 无 | 是 | connector消费的topic名称 |
| input_format | json | 是 | 不同的消费模式，具体见使用示例 |
| whole_message_info | true | 否 | 是否需要在holo表中写入message的所有相关信息，为true则可能需要设置相关字段名称，详见下方注释[1] |
| connection.jdbcUrl | 无 | 是 | Hologres实时数据API的jdbcUrl，包含数据库名称 |
| connection.username | 无 | 是 | 阿里云账号的AccessKey ID |
| connection.password | 无 | 是 | 阿里云账号的Accesskey SECRET |
| table | 无 | 是 | Hologres用于接收数据的表名称 |
| connection.writeMode | INSERT_OR_REPLACE | 否 | 当INSERT目标表为有主键的表时采用不同策略:<br>INSERT_OR_IGNORE 当主键冲突时，不写入<br>INSERT_OR_UPDATE 当主键冲突时，更新相应列<br>INSERT_OR_REPLACE 当主键冲突时，更新所有列|
| connection.writeBatchSize | 512 | 否 | 每个写入线程的最大批次大小，<br>在经过WriteMode合并后的Put数量达到writeBatchSize时进行一次批量提交 |
| connection.writeBatchByteSize | 2097152（2 * 1024 * 1024） | 否 | 每个写入线程的最大批次bytes大小，单位为Byte，默认2MB，<br>在经过WriteMode合并后的Put数据字节数达到writeBatchByteSize时进行一次批量提交 |
| connection.rewriteSqlMaxBatchSize | 1024 | 否 | 单条sql进行INSERT/DELETE操作的最大批次大小,<br>比如写入操作，所攒的批会通过 writeBatchSize/rewriteSqlMaxBatchSize 条INSERT语句完成插入 |
| connection.writeMaxIntervalMs | 10000 | 否 | 距离上次提交超过writeMaxIntervalMs会触发一次批量提交 |
| connection.writeFailStrategy | TYR_ONE_BY_ONE | 否 | 当发生写失败时的重试策略:<br>TYR_ONE_BY_ONE 当某一批次提交失败时，会将批次内的记录逐条提交（保序），其中某单条提交失败的记录将会跟随异常被抛出<br> NONE 直接抛出异常 |
| connection.writeThreadSize | 1 | 否 | 写入并发线程数（每个并发占用1个数据库连接） |
| connection.dynamicPartition| false|	否 |若为true，写入分区表父表时，当分区不存在时自动创建分区 |
| connection.retryCount | 3 | 否 | 当连接故障时，写入和查询的重试次数 |
| connection.retrySleepInitMs | 1000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| connection.retrySleepStepMs | 10000 | 否 | 每次重试的等待时间=retrySleepInitMs+retry*retrySleepStepMs |
| connection.connectionMaxIdleMs | 60000 | 否 | 写入线程和点查线程数据库连接的最大Idle时间，超过连接将被释放 |
| initial_timestamp | -1 | 否 | 从某个时间点开始消费kafka数据写入hologres |
| dirty_date_strategy | EXCEPTION | 否 | 脏数据处理策略，只对空数据或者schema错误的脏数据有效，不能处理格式错误的数据（input_format错误，乱码等）<br>EXCEPTION: 脏数据抛出异常 <br>SKIP: 跳过脏数据，打印warn日志<br>SKIP_ONCE: 跳过特定的一条脏数据，详见dirty_date_to_skip_once参数 |
| dirty_date_to_skip_once | `null,-1,-1` | 否 | 在dirty_date_strategy=SKIP_ONCE 时生效，由三个部分组成，分别是需要跳过的脏数据对应的topic，partition，offset，通过`,`隔开 |
| metrics_report_interval | 60 | 否 | metrics report间隔，单位为s<br>设置为 -1 表示不开启 |


> 注释[1]:

写入message相关信息，可以设置相关字段的名称

| 参数名 | 默认值 | 是否必填 | 说明 |
| :---: | :---: | :---: |:---: |
| message_topic | kafkatopic | 否 | message的topic在hologres中字段名 |
| message_partition | kafkapartition | 否 | message的分区在hologres中字段名 |
| message_offset | kafkaoffset | 否 | message的offset在hologres中字段名 |
| message_timestamp | kafkatimestamp | 否 | message生成时的时间戳在hologres中的字段名 |
